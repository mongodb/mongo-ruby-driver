# frozen_string_literal: true
# encoding: utf-8

autoload :JSON, 'json'
autoload :FileUtils, 'fileutils'
autoload :Find, 'find'

module Mrss

  autoload :ChildProcessHelper, 'mrss/child_process_helper'

  # Organizes and runs all of the tests in the test suite in batches.
  #
  # Organizing the tests in batches serves two purposes:
  #
  # 1. This allows running unit tests before integration tests, therefore
  #    in theory revealing failures quicker on average.
  # 2. This allows running some tests that have high intermittent failure rate
  #    in their own test process.
  #
  # This class aggregates RSpec results after the test runs.
  class SpecOrganizer

    class BucketsNotPrioritized < StandardError
    end

    def initialize(root: nil, classifiers:, priority_order:,
      spec_root: nil, rspec_json_path: nil, rspec_all_json_path: nil, rspec_xml_path: nil, randomize: false
    )
      @spec_root = spec_root || File.join(root, 'spec')
      @classifiers = classifiers
      @priority_order = priority_order
      @rspec_json_path = rspec_json_path || File.join(root, 'tmp/rspec.json')
      @rspec_all_json_path = rspec_all_json_path || File.join(root, 'tmp/rspec-all.json')
      @rspec_xml_path = rspec_xml_path || File.join(root, 'tmp/rspec.xml')
      @randomize = !!randomize
    end

    attr_reader :spec_root, :classifiers, :priority_order
    attr_reader :rspec_json_path, :rspec_all_json_path, :rspec_xml_path

    def randomize?
      @randomize
    end

    def seed
      @seed ||= (rand * 100_000).to_i
    end

    # Remove all XML files from tmp directory before running tests
    def cleanup_xml_files
      xml_pattern = File.join(File.dirname(rspec_xml_path), '*.xml')
      Dir.glob(xml_pattern).each do |xml_file|
        FileUtils.rm_f(xml_file)
      end
    end

    # Move the XML file to a timestamped version for evergreen upload
    def archive_xml_file(category)
      return unless File.exist?(rspec_xml_path)

      timestamp = Time.now.strftime('%Y%m%d_%H%M%S_%3N')
      archived_path = rspec_xml_path.sub(/\.xml$/, "-#{category}-#{timestamp}.xml")

      FileUtils.mv(rspec_xml_path, archived_path)
      puts "Archived XML results to #{archived_path}"
    end

    def buckets
      @buckets ||= {}.tap do |buckets|
        Find.find(spec_root) do |path|
          next unless File.file?(path)
          next unless path =~ /_spec\.rb\z/
          rel_path = path[(spec_root.length + 1)..path.length]

          found = false
          classifiers.each do |(regexp, category)|
            if regexp =~ rel_path
              buckets[category] ||= []
              buckets[category] << File.join('spec', rel_path)
              found = true
              break
            end
          end

          unless found
            buckets[nil] ||= []
            buckets[nil] << File.join('spec', rel_path)
          end
        end
      end.freeze
    end

    def ordered_buckets
      @ordered_buckets ||= {}.tap do |ordered_buckets|
        buckets = self.buckets.dup
        priority_order.each do |category|
          files = buckets.delete(category)
          ordered_buckets[category] = files
        end

        if files = buckets.delete(nil)
          ordered_buckets[nil] = files
        end

        unless buckets.empty?
          raise BucketsNotPrioritized, "Some buckets were not prioritized: #{buckets.keys.map(&:to_s).join(', ')}"
        end
      end.freeze
    end

    def run
      run_buckets(*buckets.keys)
    end

    def run_buckets(*buckets)
      FileUtils.rm_f(rspec_all_json_path)
      # Clean up all XML files before starting test runs
      cleanup_xml_files

      buckets.each do |bucket|
        if bucket && !self.buckets[bucket]
          raise "Unknown bucket #{bucket}"
        end
      end
      buckets = Hash[self.buckets.select { |k, v| buckets.include?(k) }]

      failed = []

      priority_order.each do |category|
        if files = buckets.delete(category)
          unless run_files(category, files)
            failed << category
          end
        end
      end
      if files = buckets.delete(nil)
        unless run_files('remaining', files)
          failed << 'remaining'
        end
      end

      unless buckets.empty?
        raise "Some buckets were not executed: #{buckets.keys.map(&:to_s).join(', ')}"
      end

      if failed.any?
        raise "The following buckets failed: #{failed.map(&:to_s).join(', ')}"
      end
    end

    def run_files(category, paths)
      puts "Running #{category.to_s.gsub('_', ' ')} tests"
      FileUtils.rm_f(rspec_json_path)
      FileUtils.rm_f(rspec_xml_path)  # Clean up XML file before running this bucket

      cmd = %w(rspec) + paths
      # Add junit formatter for XML output
      cmd += ['--format', 'RspecJunitFormatter', '--out', rspec_xml_path]

      if randomize?
        cmd += %W(--order rand:#{seed})
      end

      begin
        puts "Running #{cmd.join(' ')}"
        ChildProcessHelper.check_call(cmd)
      ensure
        if File.exist?(rspec_json_path)
          if File.exist?(rspec_all_json_path)
            merge_rspec_results
          else
            FileUtils.cp(rspec_json_path, rspec_all_json_path)
          end
        end

        # Archive XML file after running this bucket
        archive_xml_file(category)
      end

      true
    rescue ChildProcessHelper::SpawnError
      false
    end

    def merge_rspec_results
      all = JSON.parse(File.read(rspec_all_json_path))
      new = JSON.parse(File.read(rspec_json_path))
      all['examples'] += new.delete('examples')
      new.delete('summary').each do |k, v|
        all['summary'][k] += v
      end
      new.delete('version')
      new.delete('summary_line')
      # The spec organizer runs all buckets with the same seed, hence
      # we can drop the seed from new results.
      new.delete('seed')
      unless new.empty?
        raise "Unhandled rspec results keys: #{new.keys.join(', ')}"
      end
      # We do not merge summary lines, delete them from aggregated results
      all.delete('summary_line')
      File.open(rspec_all_json_path, 'w') do |f|
        f << JSON.dump(all)
      end
    end
  end
end

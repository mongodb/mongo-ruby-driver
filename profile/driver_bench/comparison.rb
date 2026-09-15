# frozen_string_literal: true

require 'fileutils'
require 'json'
require 'tmpdir'

require_relative 'configuration'
require_relative 'percentiles'
require_relative 'suite'

module Mongo
  module DriverBench
    # Runs the DriverBench micro-benchmarks under several driver
    # configurations and reports what each configuration costs relative to the
    # baseline.
    #
    # OpenTelemetry can only be installed into a process once, and the
    # "api-only" configuration requires that the SDK was never loaded, so each
    # configuration is measured in its own subprocess.
    #
    # Configurations are run interleaved -- the whole set, then the whole set
    # again -- rather than one configuration to completion and then the next.
    # The quantity being measured is a difference between two configurations,
    # and a machine that slows down halfway through a run would otherwise put
    # all of one configuration's samples in the fast half and all of another's
    # in the slow half, which shows up as overhead that is not there.
    #
    # Parameterised by the environment:
    #
    #   CONFIGURATIONS  comma-separated configuration names (default: all)
    #   REPS            repetitions of the whole set (default 1)
    #   MONGODB_URI     server to benchmark
    #   DRIVER_BENCH_TASKS
    #   DRIVER_BENCH_EXCLUDE_TASKS
    #                   narrow the micro-benchmarks, as for Suite. When
    #                   neither is set, BSON micro-benchmarks are skipped:
    #                   they never talk to a server, so they create no spans
    #                   and can only add noise to the comparison.
    #   PERFORMANCE_RESULTS_FILE
    #                   where the combined results are written
    #
    # @api private
    class Comparison
      DEFAULT_EXCLUDED_TASKS = 'BSON'
      DEFAULT_RESULTS_FILE = 'perf-comparison.json'

      TABLE_FORMAT = '%-32<task>s  %14<baseline>s  %14<score>s  %9<loss>s'

      def self.run!
        new.run
      end

      def initialize(env = ENV)
        @env = env
        @reps = Integer(env['REPS'] || 1)
        @configurations = requested_configurations
        @scores = Hash.new { |hash, key| hash[key] = [] }
      end

      # Runs every configuration, @reps times, and reports the comparison.
      #
      # @return [ String ] the summary table.
      def run
        raise 'the baseline configuration is required to compare against' unless
          @configurations.any?(&:baseline?)

        Dir.mktmpdir('driver-bench') do |dir|
          1.upto(@reps) do |rep|
            @configurations.each { |configuration| measure(configuration, rep, dir) }
          end
        end

        save_perf_data(compile_perf_data)
        summarize
      end

      private

      def requested_configurations
        names = (@env['CONFIGURATIONS'] || '').split(',').map(&:strip).reject(&:empty?)
        return Configuration::ALL if names.empty?

        names.map { |name| Configuration[name] }
      end

      def baseline
        @baseline ||= @configurations.find(&:baseline?)
      end

      # Runs one configuration once, in its own process, and records the score
      # of every micro-benchmark it reported.
      def measure(configuration, rep, dir)
        results_file = File.join(dir, "#{configuration.name}-#{rep}.json")

        puts format("\n----- rep %d/%d: %s -----", rep, @reps, configuration.name)
        unless system(child_env(configuration, results_file), 'bundle', 'exec', 'rake', 'driver_bench:run')
          raise "configuration #{configuration.name} failed in rep #{rep}"
        end

        JSON.parse(File.read(results_file)).each do |entry|
          score = entry['metrics'].find { |metric| metric['name'] == 'score' }
          @scores[[ entry['info']['test_name'], configuration.name ]] << score['value']
        end
      end

      def child_env(configuration, results_file)
        {
          Configuration::ENV_VAR => configuration.name,
          'PERFORMANCE_RESULTS_FILE' => results_file,
          'DRIVER_BENCH_EXCLUDE_TASKS' => excluded_tasks
        }
      end

      def excluded_tasks
        return @env['DRIVER_BENCH_EXCLUDE_TASKS'].to_s if
          @env['DRIVER_BENCH_EXCLUDE_TASKS'] || @env['DRIVER_BENCH_TASKS']

        DEFAULT_EXCLUDED_TASKS
      end

      # The score for one micro-benchmark under one configuration: the median
      # across repetitions, for the same reason the spec takes the median
      # across iterations.
      def score_for(task, configuration)
        samples = @scores[[ task, configuration.name ]]
        return nil if samples.empty?

        Percentiles.new(samples)[50]
      end

      # Scores are throughput, so enabling a feature shows up as a loss.
      #
      # @return [ Float | nil ] the percentage of throughput given up,
      #   relative to the baseline.
      def loss_for(task, configuration)
        reference = score_for(task, baseline)
        score = score_for(task, configuration)
        return nil if reference.nil? || score.nil? || reference.zero?

        (reference - score) / reference * 100.0
      end

      def tasks
        @tasks ||= @scores.keys.map(&:first).uniq
      end

      def compile_perf_data
        tasks.flat_map do |task|
          @configurations.filter_map do |configuration|
            score = score_for(task, configuration)
            next if score.nil?

            metrics = [ { 'name' => 'score', 'value' => score } ]
            loss = loss_for(task, configuration)
            # The loss is recorded as a metric of its own, rather than left to
            # be derived by comparing two time series later, so that it can be
            # watched for regressions directly and so that host-to-host
            # variation cancels out of it.
            metrics << { 'name' => 'overhead_pct', 'value' => loss } unless
              configuration.baseline? || loss.nil?

            {
              'info' => {
                'test_name' => task,
                'args' => { 'configuration' => configuration.name }
              },
              'metrics' => metrics
            }
          end
        end
      end

      def save_perf_data(data, file_name: @env['PERFORMANCE_RESULTS_FILE'] || DEFAULT_RESULTS_FILE)
        File.write(file_name, data.to_json)
      end

      def summarize
        lines = [ format("\n===== Configuration comparison (%d rep%s, median) =====",
                         @reps, (@reps == 1) ? '' : 's') ]

        @configurations.reject(&:baseline?).each do |configuration|
          lines << ''
          lines << format('--- %s vs %s ---', configuration.name, baseline.name)
          lines << configuration.description
          lines << format(TABLE_FORMAT,
                          task: 'micro-benchmark', baseline: "#{baseline.name} MB/s",
                          score: 'MB/s', loss: 'loss %')
          tasks.each { |task| lines << row(task, configuration) }
        end

        lines.join("\n")
      end

      def row(task, configuration)
        loss = loss_for(task, configuration)

        format(TABLE_FORMAT,
               task: task,
               baseline: format('%.4g', score_for(task, baseline)),
               score: format('%.4g', score_for(task, configuration)),
               loss: loss.nil? ? 'n/a' : format('%+.2f', loss))
      end
    end
  end
end

# frozen_string_literal: true

require_relative 'bson'
require_relative 'configuration'
require_relative 'multi_doc'
require_relative 'parallel'
require_relative 'single_doc'

module Mongo
  module DriverBench
    ALL = [ *BSON::ALL, *SingleDoc::ALL, *MultiDoc::ALL, *Parallel::ALL ].freeze

    BENCHES = {
      'BSONBench' => BSON::BENCH,
      'SingleBench' => SingleDoc::BENCH,
      'MultiBench' => MultiDoc::BENCH,
      'ParallelBench' => Parallel::BENCH,

      'ReadBench' => [
        SingleDoc::FindOneByID,
        MultiDoc::FindMany,
        MultiDoc::GridFS::Download,
        Parallel::LDJSON::Export,
        Parallel::GridFS::Download
      ].freeze,

      'WriteBench' => [
        SingleDoc::InsertOne::SmallDoc,
        SingleDoc::InsertOne::LargeDoc,
        MultiDoc::BulkInsert::SmallDoc,
        MultiDoc::BulkInsert::LargeDoc,
        MultiDoc::GridFS::Upload,
        Parallel::LDJSON::Import,
        Parallel::GridFS::Upload
      ].freeze
    }.freeze

    # A benchmark suite for running all benchmarks and aggregating (and
    # reporting) the results.
    #
    # @api private
    class Suite
      PERCENTILES = [ 10, 25, 50, 75, 90, 95, 98, 99 ].freeze

      def self.run!
        new.run
      end

      def run
        configuration.install!
        announce_configuration

        perf_data = []
        benches = Hash.new { |h, k| h[k] = [] }

        tasks.each do |klass|
          result = run_benchmark(klass)
          perf_data << compile_perf_data(result)
          append_to_benchmarks(klass, result, benches)
        end

        # The composites average a fixed list of micro-benchmarks, so they are
        # only meaningful when every micro-benchmark ran.
        perf_data += compile_benchmarks(benches) if tasks == ALL

        save_perf_data(perf_data)
        summarize_perf_data(perf_data)
      end

      # The micro-benchmarks to run.
      #
      # By default this is every micro-benchmark, but DRIVER_BENCH_TASKS and
      # DRIVER_BENCH_EXCLUDE_TASKS narrow it to those whose names do (or do
      # not) contain one of a comma-separated list of substrings. Narrowing
      # the list is what makes an optimize-and-remeasure loop practical: the
      # full suite spends at least a minute per micro-benchmark per
      # configuration.
      #
      # @return [ Array<Class> ] the micro-benchmark classes to run.
      def tasks
        @tasks ||= begin
          included = patterns('DRIVER_BENCH_TASKS')
          excluded = patterns('DRIVER_BENCH_EXCLUDE_TASKS')

          selected = ALL.select do |klass|
            name = klass.bench_name.downcase
            (included.empty? || included.any? { |pattern| name.include?(pattern) }) &&
              excluded.none? { |pattern| name.include?(pattern) }
          end

          raise 'no micro-benchmarks match the requested task filters' if selected.empty?

          (selected == ALL) ? ALL : selected.freeze
        end
      end

      private

      def configuration
        @configuration ||= Configuration.current
      end

      def patterns(variable)
        (ENV[variable] || '').split(',').map { |pattern| pattern.strip.downcase }.reject(&:empty?)
      end

      def announce_configuration
        puts format('===== DriverBench: %s =====', configuration.name)
        puts configuration.description
        puts format('%d of %d micro-benchmarks', tasks.length, ALL.length) unless tasks == ALL
        puts
      end

      def run_benchmark(klass)
        print klass.bench_name, ': '
        $stdout.flush

        klass.new.run.tap do |result|
          puts format('%4.4g', result[:score])
        end
      end

      def compile_perf_data(result)
        percentile_data = PERCENTILES.map do |percentile|
          { 'name' => "time-#{percentile}%",
            'value' => result[:percentiles][percentile] }
        end

        {
          'info' => {
            'test_name' => result[:name],
            'args' => { 'configuration' => configuration.name },
          },
          'metrics' => [
            { 'name' => 'score',
              'value' => result[:score] },
            *percentile_data
          ]
        }
      end

      def append_to_benchmarks(klass, result, benches)
        BENCHES.each do |benchmark, list|
          benches[benchmark] << result[:score] if list.include?(klass)
        end
      end

      def compile_benchmarks(benches)
        benches.each_key do |key|
          benches[key] = benches[key].sum / benches[key].length
        end

        benches['DriverBench'] = (benches['ReadBench'] + benches['WriteBench']) / 2

        benches.map do |bench, score|
          {
            'info' => {
              'test_name' => bench,
              'args' => { 'configuration' => configuration.name }
            },
            'metrics' => [
              { 'name' => 'score',
                'value' => score }
            ]
          }
        end
      end

      def summarize_perf_data(data)
        puts format('===== Performance Results (%s) =====', configuration.name)
        data.each do |item|
          puts format('%s : %4.4g', item['info']['test_name'], item['metrics'][0]['value'])
          next unless item['metrics'].length > 1

          item['metrics'].each do |metric|
            next if metric['name'] == 'score'

            puts format('  %s : %4.4g', metric['name'], metric['value'])
          end
        end
      end

      def save_perf_data(data, file_name: ENV['PERFORMANCE_RESULTS_FILE'] || 'results.json')
        File.write(file_name, data.to_json)
      end
    end
  end
end

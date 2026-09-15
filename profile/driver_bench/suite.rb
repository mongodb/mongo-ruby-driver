# frozen_string_literal: true

require 'time'

require_relative 'bson'
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

      # The name of the primary metric reported for every benchmark and
      # composite. Matches the name used by the other drivers.
      SCORE_METRIC = 'megabytes_per_second'

      def self.run!
        new.run
      end

      def run
        perf_data = []
        benches = Hash.new { |h, k| h[k] = [] }

        started_at = Time.now.utc

        ALL.each do |klass|
          result = run_benchmark(klass)
          perf_data << compile_perf_data(result)
          append_to_benchmarks(klass, result, benches)
        end

        # The composites are derived from every micro-benchmark, so they are
        # timestamped with the span of the whole suite rather than of a single
        # benchmark.
        perf_data += compile_benchmarks(benches, started_at, Time.now.utc)

        save_perf_data(perf_data)
        summarize_perf_data(perf_data)
      end

      private

      def run_benchmark(klass)
        print klass.bench_name, ': '
        $stdout.flush

        klass.new.run.tap do |result|
          puts format('%4.4g', result[:score])
        end
      end

      def compile_perf_data(result)
        percentile_data = PERCENTILES.map do |percentile|
          # Percentiles are wall-clock iteration times, so a smaller number is
          # an improvement -- the opposite of the throughput score.
          metric("time-#{percentile}%", result[:percentiles][percentile],
                 direction: 'down', unit: 'seconds')
        end

        {
          'info' => {
            'test_name' => result[:name],
            'args' => {},
          },
          'created_at' => iso8601(result[:started_at]),
          'completed_at' => iso8601(result[:completed_at]),
          'metrics' => [
            score_metric(result[:score]),
            *percentile_data
          ]
        }
      end

      # The primary metric for every benchmark, named to match the other
      # drivers (see the Node and Go implementations) so that the numbers are
      # comparable across the performance analytics backend.
      def score_metric(score)
        metric(SCORE_METRIC, score, direction: 'up', unit: 'megabytes_per_second')
      end

      # Builds a single metric entry in the format expected by the Signal
      # Processing Service. The metadata drives how the change point detector
      # reports a shift: improvement_direction says which way is better.
      def metric(name, value, direction:, unit:)
        {
          'name' => name,
          'value' => value,
          'metadata' => {
            'improvement_direction' => direction,
            'measurement_unit' => unit
          }
        }
      end

      def iso8601(time)
        (time || Time.now.utc).utc.iso8601
      end

      def append_to_benchmarks(klass, result, benches)
        BENCHES.each do |benchmark, list|
          benches[benchmark] << result[:score] if list.include?(klass)
        end
      end

      def compile_benchmarks(benches, started_at, completed_at)
        benches.each_key do |key|
          benches[key] = benches[key].sum / benches[key].length
        end

        benches['DriverBench'] = (benches['ReadBench'] + benches['WriteBench']) / 2

        benches.map do |bench, score|
          {
            'info' => {
              'test_name' => bench,
              'args' => {}
            },
            'created_at' => iso8601(started_at),
            'completed_at' => iso8601(completed_at),
            'metrics' => [ score_metric(score) ]
          }
        end
      end

      def summarize_perf_data(data)
        puts '===== Performance Results ====='
        data.each do |item|
          puts format('%s : %4.4g', item['info']['test_name'], item['metrics'][0]['value'])
          next unless item['metrics'].length > 1

          item['metrics'].each do |metric|
            next if metric['name'] == SCORE_METRIC

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

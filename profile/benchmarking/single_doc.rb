module Mongo
  module Benchmarking
    module SingleDoc

      # The number of repetitions of the test to do before timing.
      #
      # @return [ Integer ] The number of warmup repetitions.
      #
      # @since 2.2.2
      WARMUP_REPETITIONS = 10

      # The number of times to run and time the test.
      #
      # @return [ Integer ] The number of test repetitions.
      #
      # @since 2.2.2
      TEST_REPETITIONS = 5

      extend self

      # Run a micro benchmark test.
      #
      # @example Run a test.
      #   Benchmarking::Micro.run(:flat)
      #
      # @param [ Symbol ] The type of test to run.
      #
      # @return [ Array ] An array of results.
      #
      # @since 2.2.2
      def run(type)
        Mongo::Logger.logger.level = ::Logger::WARN
        puts "#{type} : #{send(type, TEST_REPETITIONS)}"
      end

      private

      def client
        @client ||= Mongo::Client.new(["localhost:27017"], database: 'perftest')
      end

      def collection
        @collection ||= client[:corpus]
      end

      def command(repetitions)
        monitor = client.cluster.servers.first.monitor
        results = repetitions.times.collect do
          Benchmark.realtime do
            10_000.times do
              monitor.connection.ismaster
            end
          end
        end
        Benchmarking.median(results)
      end
    end
  end
end

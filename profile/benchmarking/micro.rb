module Mongo
  module Benchmarking
    module Micro

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
      TEST_REPETITIONS = 100

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
        file = type.to_s << "_bson.json"
        file_path = MICRO_TESTS_PATH + file
        ['encode', 'decode'].collect do |method|
          result = send(method, file_path, TEST_REPETITIONS)
          puts "#{method} : #{result}"
        end
      end

      # Run an encoding micro benchmark test.
      #
      # @example Run an encoding test.
      #   Benchmarking::Micro.encode(file_name, 100)
      #
      # @param [ String ] The name of the file with data for the test.
      # @param [ Integer ] The number of test repetitions.
      #
      # @return [ Numeric ] The median of the results.
      #
      # @since 2.2.2
      def encode(file_name, repetitions)
        data = Benchmarking.load_file(file_name)
        doc = BSON::Document.new(data.first)

        # WARMUP_REPETITIONS.times do
        #   doc.to_bson
        # end

        Benchmarking.median(repetitions.times.collect do
          Benchmark.realtime do
            10_000.times do
              doc.to_bson
            end
          end
        end)
      end

      # Run a decoding micro benchmark test.
      #
      # @example Run an decoding test.
      #   Benchmarking::Micro.decode(file_name, 100)
      #
      # @param [ String ] The name of the file with data for the test.
      # @param [ Integer ] The number of test repetitions.
      #
      # @return [ Numeric ] The median of the results.
      #
      # @since 2.2.2
      def decode(file_name, repetitions)
        data = Benchmarking.load_file(file_name)
        buffer = BSON::Document.new(data.first).to_bson

        # WARMUP_REPETITIONS.times do
        #   BSON::Document.from_bson(buffers.shift)
        # end

        results = repetitions.times.collect do
          Benchmark.realtime do
            10_000.times do
              buffer.reset_read_position
              BSON::Document.from_bson(buffer)
            end
          end
        end

        Benchmarking.median(results)
      end
    end
  end
end

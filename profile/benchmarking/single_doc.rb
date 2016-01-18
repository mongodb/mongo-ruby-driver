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
      TEST_REPETITIONS = 1

      # The file containing the single tweet document.
      #
      # @return [ String ] The file containing the tweet document.
      #
      # @since 2.2.2
      TWEET_DOCUMENT_FILE = TWEET_DOCUMENT_PATH + 'TWEET.json'

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

      def find_one_by_id(repetitions)
        client.database.drop
        doc = tweet_document

        10_000.times do |i|
          doc[:_id] = i
          collection.insert_one(doc)
        end

        results = repetitions.times.collect do
          Benchmark.realtime do
            10_000.times do |i|
              collection.find({ _id: i }, limit: -1).first
            end
          end
        end
        Benchmarking.median(results)
      end

      def client
        @client ||= Mongo::Client.new(["localhost:27017"], database: 'perftest')
      end

      def collection
        @collection ||= client[:corpus]
      end

      def tweet_document
        Benchmarking.load_file(TWEET_DOCUMENT_FILE).first
      end
    end
  end
end

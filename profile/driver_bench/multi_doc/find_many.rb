# frozen_string_literal: true

require_relative 'base'

module Mongo
  module DriverBench
    module MultiDoc
      # "This benchmark tests driver performance retrieving multiple documents
      # from a query."
      #
      # @api private
      class FindMany < Mongo::DriverBench::MultiDoc::Base
        bench_name 'Find many and empty the cursor'

        private

        def file_name
          'single_and_multi_document/tweet.json'
        end

        def setup
          super

          prototype = dataset.first
          10.times do
            docs = Array.new(1000, prototype)
            @collection.insert_many(docs)
          end
        end

        def do_task
          collection.find.each do |result|
            # discard the result
          end
        end
      end
    end
  end
end

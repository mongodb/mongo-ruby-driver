# frozen_string_literal: true

require_relative 'base'

module Mongo
  module DriverBench
    module SingleDoc
      # "This benchmark tests driver performance sending an indexed query to
      # the database and reading a single document in response."
      #
      # @api private
      class FindOneByID < Mongo::DriverBench::SingleDoc::Base
        bench_name 'Find one by ID'

        def file_name
          'single_and_multi_document/tweet.json'
        end

        def setup
          super

          docs = Array.new(10_000) { |i| dataset.merge(_id: i + 1) }
          @collection.insert_many(docs)
        end

        def do_task
          10_000.times do |i|
            collection.find(_id: i + 1).to_a
          end
        end
      end
    end
  end
end

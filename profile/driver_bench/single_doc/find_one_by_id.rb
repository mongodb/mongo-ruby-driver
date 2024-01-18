# frozen_string_literal: true

require_relative 'base'

module Mongo
  module DriverBench
    module SingleDoc
      class FindOneByID < Mongo::DriverBench::SingleDoc::Base
        def file_name
          "single_and_multi_document/tweet.json"
        end

        def setup
          super

          doc = dataset
          10_000.times do |i|
            @collection.insert_one(doc.merge(_id: i + 1))
          end
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

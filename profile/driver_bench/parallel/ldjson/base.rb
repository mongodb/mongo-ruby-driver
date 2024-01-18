# frozen_string_literal: true

require_relative '../base'

module Mongo
  module DriverBench
    module Parallel
      module LDJSON
        class Base < Mongo::DriverBench::Parallel::Base
          def file_name_at(index)
            format("parallel/ldjson_multi/ldjson%03d.txt", index)
          end

          private

          attr_reader :collection

          def insert_docs_from_file(file_name, ids_relative_to: nil)
            next_id = ids_relative_to
            docs = File.readlines(path_to_file(file_name)).map do |line|
              JSON.parse(line).tap do |doc|
                if ids_relative_to
                  doc['_id'] = next_id
                  next_id += 1
                end
              end
            end

            collection.insert_many(docs)
          end

          def prepare_collection
            @collection = @client.database[:corpus].tap do |corpus|
              corpus.drop
              corpus.create
            end
          end
        end
      end
    end
  end
end

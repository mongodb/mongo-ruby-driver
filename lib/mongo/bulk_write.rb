# Copyright (C) 2014-2015 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

require 'mongo/bulk_write/result'
require 'mongo/bulk_write/transformable'
require 'mongo/bulk_write/validatable'
require 'mongo/bulk_write/combineable'
require 'mongo/bulk_write/ordered_combiner'
require 'mongo/bulk_write/unordered_combiner'
require 'mongo/bulk_write/result_combiner'

module Mongo
  class BulkWrite
    extend Forwardable
    include Retryable

    # @return [ Mongo::Collection ] collection The collection.
    attr_reader :collection

    # @return [ Array<Hash, BSON::Document> ] requests The requests.
    attr_reader :requests

    # @return [ Hash, BSON::Document ] options The options.
    attr_reader :options

    # Delegate various methods to the collection.
    def_delegators :@collection, :database, :cluster, :next_primary

    def_delegators :database, :client

    # Execute the bulk write operation.
    #
    # @example Execute the bulk write.
    #   bulk_write.execute
    #
    # @return [ Mongo::BulkWrite::Result ] The result.
    #
    # @since 2.1.0
    def execute
      operation_id = Monitoring.next_operation_id
      result_combiner = ResultCombiner.new
      write_with_retry do
        server = next_primary
        operations.each do |operation|
          execute_operation(
            operation.keys.first,
            operation.values.first,
            server,
            operation_id,
            result_combiner
          )
        end
      end
      result_combiner.result
    end

    # Create the new bulk write operation.
    #
    # @api private
    #
    # @example Create an ordered bulk write.
    #   Mongo::BulkWrite.new(collection, [{ insert_one: { _id: 1 }}])
    #
    # @example Create an unordered bulk write.
    #   Mongo::BulkWrite.new(collection, [{ insert_one: { _id: 1 }}], ordered: false)
    #
    # @example Create an ordered mixed bulk write.
    #   Mongo::BulkWrite.new(
    #     collection,
    #     [
    #       { insert_one: { _id: 1 }},
    #       { update_one: { filter: { _id: 0 }, update: { '$set' => { name: 'test' }}}},
    #       { delete_one: { filter: { _id: 2 }}}
    #     ]
    #   )
    #
    # @param [ Mongo::Collection ] collection The collection.
    # @param [ Array<Hash, BSON::Document> ] requests The requests.
    # @param [ Hash, BSON::Document ] options The options.
    #
    # @since 2.1.0
    def initialize(collection, requests, options = {})
      @collection = collection
      @requests = requests
      @options = options || {}
    end

    # Is the bulk write ordered?
    #
    # @api private
    #
    # @example Is the bulk write ordered?
    #   bulk_write.ordered?
    #
    # @return [ true, false ] If the bulk write is ordered.
    #
    # @since 2.1.0
    def ordered?
      @ordered ||= options.fetch(:ordered, true)
    end

    # Get the write concern for the bulk write.
    #
    # @api private
    #
    # @example Get the write concern.
    #   bulk_write.write_concern
    #
    # @return [ WriteConcern ] The write concern.
    #
    # @since 2.1.0
    def write_concern
      @write_concern ||= options[:write_concern] ?
        WriteConcern.get(options[:write_concern]) : collection.write_concern
    end

    private

    def base_spec(operation_id)
      {
        :db_name => database.name,
        :coll_name => collection.name,
        :write_concern => write_concern,
        :ordered => ordered?,
        :operation_id => operation_id,
        :bypass_document_validation => !!options[:bypass_document_validation],
        :options => options,
        :id_generator => client.options[:id_generator]
      }
    end

    def execute_operation(name, values, server, operation_id, combiner)
      begin
        if values.size > server.max_write_batch_size
          split_execute(name, values, server, operation_id, combiner)
        else
          combiner.combine!(send(name, values, server, operation_id), values.size)
        end
      rescue Error::MaxBSONSize, Error::MaxMessageSize => e
        raise e if values.size <= 1
        split_execute(name, values, server, operation_id, combiner)
      end
    end

    def operations
      if ordered?
        OrderedCombiner.new(requests).combine
      else
        UnorderedCombiner.new(requests).combine
      end
    end

    def split_execute(name, values, server, operation_id, combiner)
      execute_operation(name, values.shift(values.size / 2), server, operation_id, combiner)
      execute_operation(name, values, server, operation_id, combiner)
    end

    def delete(documents, server, operation_id)
      Operation::Write::Bulk::Delete.new(
        base_spec(operation_id).merge(:deletes => documents)
      ).execute(server.context)
    end

    alias :delete_one :delete
    alias :delete_many :delete

    def insert_one(documents, server, operation_id)
      Operation::Write::Bulk::Insert.new(
        base_spec(operation_id).merge(:documents => documents)
      ).execute(server.context)
    end

    def update(documents, server, operation_id)
      Operation::Write::Bulk::Update.new(
        base_spec(operation_id).merge(:updates => documents)
      ).execute(server.context)
    end

    alias :replace_one :update
    alias :update_one :update
    alias :update_many :update
  end
end

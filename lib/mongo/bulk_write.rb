# Copyright (C) 2014-2020 MongoDB Inc.
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
    include Operation::ResponseHandling

    # @return [ Mongo::Collection ] collection The collection.
    attr_reader :collection

    # @return [ Array<Hash, BSON::Document> ] requests The requests.
    attr_reader :requests

    # @return [ Hash, BSON::Document ] options The options.
    attr_reader :options

    # Delegate various methods to the collection.
    def_delegators :@collection,
                   :database,
                   :cluster,
                   :write_with_retry,
                   :nro_write_with_retry,
                   :next_primary

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
      operations = op_combiner.combine

      client.send(:with_session, @options) do |session|
        operations.each do |operation|
          if single_statement?(operation)
            write_concern = write_concern(session)
            write_with_retry(session, write_concern) do |server, txn_num|
              execute_operation(
                  operation.keys.first,
                  operation.values.flatten,
                  server,
                  operation_id,
                  result_combiner,
                  session,
                  txn_num)
            end
          else
            nro_write_with_retry(session, write_concern) do |server|
              execute_operation(
                  operation.keys.first,
                  operation.values.flatten,
                  server,
                  operation_id,
                  result_combiner,
                  session)
            end
          end
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
    def write_concern(session = nil)
      @write_concern ||= options[:write_concern] ?
        WriteConcern.get(options[:write_concern]) :
        collection.write_concern_with_session(session)
    end

    private

    SINGLE_STATEMENT_OPS = [ :delete_one,
                             :update_one,
                             :insert_one ].freeze

    def single_statement?(operation)
      SINGLE_STATEMENT_OPS.include?(operation.keys.first)
    end

    def base_spec(operation_id, session)
      {
        :db_name => database.name,
        :coll_name => collection.name,
        :write_concern => write_concern(session),
        :ordered => ordered?,
        :operation_id => operation_id,
        :bypass_document_validation => !!options[:bypass_document_validation],
        :options => options,
        :id_generator => client.options[:id_generator],
        :session => session
      }
    end

    def execute_operation(name, values, server, operation_id, result_combiner, session, txn_num = nil)
      raise Error::UnsupportedCollation.new if op_combiner.has_collation && !server.with_connection { |connection| connection.features }.collation_enabled?
      raise Error::UnsupportedArrayFilters.new if op_combiner.has_array_filters && !server.with_connection { |connection| connection.features }.array_filters_enabled?
      unpin_maybe(session) do
        if values.size > server.with_connection { |connection| connection.max_write_batch_size }
          split_execute(name, values, server, operation_id, result_combiner, session, txn_num)
        else
          result = send(name, values, server, operation_id, session, txn_num)
          result_combiner.combine!(result, values.size)
        end
      end
    rescue Error::MaxBSONSize, Error::MaxMessageSize => e
      raise e if values.size <= 1
      unpin_maybe(session) do
        split_execute(name, values, server, operation_id, result_combiner, session, txn_num)
      end
    end

    def op_combiner
      @op_combiner ||= ordered? ? OrderedCombiner.new(requests) : UnorderedCombiner.new(requests)
    end

    def split_execute(name, values, server, operation_id, result_combiner, session, txn_num)
      execute_operation(name, values.shift(values.size / 2), server, operation_id, result_combiner, session, txn_num)

      txn_num = session.next_txn_num if txn_num
      execute_operation(name, values, server, operation_id, result_combiner, session, txn_num)
    end

    def delete_one(documents, server, operation_id, session, txn_num)
      spec = base_spec(operation_id, session).merge(:deletes => documents, :txn_num => txn_num)
      Operation::Delete.new(spec).bulk_execute(server, client: client)
    end

    def delete_many(documents, server, operation_id, session, txn_num)
      spec = base_spec(operation_id, session).merge(:deletes => documents)
      Operation::Delete.new(spec).bulk_execute(server, client: client)
    end

    def insert_one(documents, server, operation_id, session, txn_num)
      spec = base_spec(operation_id, session).merge(:documents => documents, :txn_num => txn_num)
      Operation::Insert.new(spec).bulk_execute(server, client: client)
    end

    def update_one(documents, server, operation_id, session, txn_num)
      spec = base_spec(operation_id, session).merge(:updates => documents, :txn_num => txn_num)
      Operation::Update.new(spec).bulk_execute(server, client: client)
    end
    alias :replace_one :update_one

    def update_many(documents, server, operation_id, session, txn_num)
      spec = base_spec(operation_id, session).merge(:updates => documents)
      Operation::Update.new(spec).bulk_execute(server, client: client)
    end
  end
end

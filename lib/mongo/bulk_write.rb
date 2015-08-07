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
require 'mongo/bulk_write/ordered_combiner'

module Mongo
  class BulkWrite
    extend Forwardable

    # The insert many model constant.
    #
    # @since 2.1.0
    INSERT_MANY = :insert_many.freeze

    # The insert one model constant.
    #
    # @since 2.1.0
    INSERT_ONE = :insert_one.freeze

    # Constant for number removed.
    #
    # @since 2.1.0
    REMOVED_COUNT = 'n_removed'.freeze

    # Constant for number inserted.
    #
    # @since 2.1.0
    INSERTED_COUNT = 'n_inserted'.freeze

    # Constant for inserted ids.
    #
    # @since 2.1.0
    INSERTED_IDS = 'inserted_ids'.freeze

    # Constant for number matched.
    #
    # @since 2.1.0
    MATCHED_COUNT = 'n_matched'.freeze

    # Constant for number modified.
    #
    # @since 2.1.0
    MODIFIED_COUNT = 'n_modified'.freeze

    # Constant for number upserted.
    #
    # @since 2.1.0
    UPSERTED_COUNT = 'n_upserted'.freeze

    # Constant for upserted ids.
    #
    # @since 2.1.0
    UPSERTED_IDS = 'upserted_ids'.freeze

    # @return [ Mongo::Collection ] collection The collection.
    attr_reader :collection

    # @return [ Array<Hash, BSON::Document> ] requests The requests.
    attr_reader :requests

    # @return [ Hash, BSON::Document ] options The options.
    attr_reader :options

    # Delegate various methods to the collection.
    def_delegators :@collection, :database, :cluster, :next_primary

    # Execute the bulk write operation.
    #
    # @example Execute the bulk write.
    #   bulk_write.execute
    #
    # @return [ Mongo::BulkWrite::Result ] The result.
    #
    # @since 2.1.0
    def execute
      server = next_primary
      operation_id = Monitoring.next_operation_id
      operations.each do |operation|
        result = send(operation.keys.first, operation.values.first, server, operation_id)
        p result
        # combine the results here.
      end
      BulkWrite::Result.new({})
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

    def operations
      if ordered?
        OrderedCombiner.new(requests).combine
      else
        UnorderedCombiner.new(requests).combine
      end
    end

    def insert_many(documents, server, operation_id)
      Operation::Write::BulkInsert.new(
        :documents => documents,
        :db_name => database.name,
        :coll_name => collection.name,
        :write_concern => write_concern,
        :ordered => ordered?,
        :operation_id => operation_id
      ).execute(server.context)
    end
  end
end

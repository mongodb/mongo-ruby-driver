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

require 'mongo/collection/view'

module Mongo

  # Represents a collection in the database and operations that can directly be
  # applied to one.
  #
  # @since 2.0.0
  class Collection
    extend Forwardable

    # @return [ Mongo::Database ] The database the collection resides in.
    attr_reader :database

    # @return [ String ] The name of the collection.
    attr_reader :name

    # @return [ Hash ] The collection options.
    attr_reader :options

    # Get client, cluster, read preference, and write concern from client.
    def_delegators :database, :client, :cluster, :read_preference, :write_concern

    # Delegate to the cluster for the next primary.
    def_delegators :cluster, :next_primary

    # Convenience delegators to find.
    def_delegators :find, :parallel_scan

    # Check if a collection is equal to another object. Will check the name and
    # the database for equality.
    #
    # @example Check collection equality.
    #   collection == other
    #
    # @param [ Object ] other The object to check.
    #
    # @return [ true, false ] If the objects are equal.
    #
    # @since 2.0.0
    def ==(other)
      return false unless other.is_a?(Collection)
      name == other.name && database == other.database && options == other.options
    end

    # Is the collection capped?
    #
    # @example Is the collection capped?
    #   collection.capped?
    #
    # @return [ true, false ] If the collection is capped.
    #
    # @since 2.0.0
    def capped?
      database.command(:collstats => name).documents[0]['capped']
    end

    # Force the collection to be created in the database.
    #
    # @example Force the collection to be created.
    #   collection.create
    #
    # @return [ Result ] The result of the command.
    #
    # @since 2.0.0
    def create
      database.command({ :create => name }.merge(options))
    end

    # Drop the collection. Will also drop all indexes associated with the
    # collection.
    #
    # @note An error returned if the collection doesn't exist is suppressed.
    #
    # @example Drop the collection.
    #   collection.drop
    #
    # @return [ Result ] The result of the command.
    #
    # @since 2.0.0
    def drop
      database.command(:drop => name)
    rescue Error::OperationFailure => ex
      raise ex unless ex.message =~ /ns not found/
      false
    end

    # Find documents in the collection.
    #
    # @example Find documents in the collection by a selector.
    #   collection.find(name: 1)
    #
    # @example Get all documents in a collection.
    #   collection.find
    #
    # @param [ Hash ] filter The filter to use in the find.
    # @param [ Hash ] options The options for the find.
    #
    # @option options [ true, false ] :allow_partial_results Allows the query to get partial
    #   results if some shards are down.
    # @option options [ Integer ] :batch_size The number of documents returned in each batch
    #   of results from MongoDB.
    # @option options [ String ] :comment Associate a comment with the query.
    # @option options [ :tailable, :tailable_await ] :cursor_type The type of cursor to use.
    # @option options [ Integer ] :limit The max number of docs to return from the query.
    # @option options [ Integer ] :max_time_ms The maximum amount of time to allow the query
    #   to run in milliseconds.
    # @option options [ Hash ] :modifiers Meta-operators modifying the output or behavior
    #   of a query.
    # @option options [ true, false ] :no_cursor_timeout The server normally times out idle
    #   cursors after an inactivity period (10 minutes) to prevent excess memory use.
    #   Set this option to prevent that.
    # @option options [ true, false ] :oplog_replay Internal replication use only - driver
    #   should not set.
    # @option options [ Hash ] :projection The fields to include or exclude from each doc
    #   in the result set.
    # @option options [ Integer ] :skip The number of docs to skip before returning results.
    # @option options [ Hash ] :sort The key and direction pairs by which the result set
    #   will be sorted.
    #
    # @return [ CollectionView ] The collection view.
    #
    # @since 2.0.0
    def find(filter = nil, options = {})
      View.new(self, filter || {}, options)
    end

    # Perform an aggregation on the collection.
    #
    # @example Perform an aggregation.
    #   collection.aggregate([ { "$group" => { "_id" => "$city", "tpop" => { "$sum" => "$pop" }}} ])
    #
    # @param [ Array<Hash> ] pipeline The aggregation pipeline.
    # @param [ Hash ] options The aggregation options.
    #
    # @option options [ true, false ] :allow_disk_use Set to true if disk usage is allowed during
    #   the aggregation..
    # @option options [ Integer ] :batch_size The number of documents to return per batch.
    # @option options [ Integer ] :max_time_ms The maximum amount of time in milliseconds to allow the
    #   aggregation to run..
    # @option options [ true, false ] :use_cursor Indicates whether the command will request that the server
    #   provide results using a cursor..
    #
    # @return [ Aggregation ] The aggregation object.
    #
    # @since 2.1.0
    def aggregate(pipeline, options = {})
      View.new(self, {}).aggregate(pipeline, options)
    end

    # Get a view of all indexes for this collection. Can be iterated or has
    # more operations.
    #
    # @example Get the index view.
    #   collection.indexes
    #
    # @param [ Hash ] options Options for getting a list of all indexes.
    #
    # @return [ View::Index ] The index view.
    #
    # @since 2.0.0
    def indexes(options = {})
      Index::View.new(self, options)
    end

    # Instantiate a new collection.
    #
    # @example Instantiate a new collection.
    #   Mongo::Collection.new(database, 'test')
    #
    # @param [ Mongo::Database ] database The collection's database.
    # @param [ String, Symbol ] name The collection name.
    # @param [ Hash ] options The collection options.
    #
    # @since 2.0.0
    def initialize(database, name, options = {})
      raise Error::InvalidCollectionName.new unless name
      @database = database
      @name = name.to_s.freeze
      @options = options.freeze
    end

    # Get a pretty printed string inspection for the collection.
    #
    # @example Inspect the collection.
    #   collection.inspect
    #
    # @return [ String ] The collection inspection.
    #
    # @since 2.0.0
    def inspect
      "#<Mongo::Collection:0x#{object_id} namespace=#{namespace}>"
    end

    # Insert a single document into the collection.
    #
    # @example Insert a document into the collection.
    #   collection.insert_one({ name: 'test' })
    #
    # @param [ Hash ] document The document to insert.
    # @param [ Hash ] options The insert options.
    #
    # @return [ Result ] The database response wrapper.
    #
    # @since 2.0.0
    def insert_one(document, options = {})
      Operation::Write::Insert.new(
        :documents => [ document ],
        :db_name => database.name,
        :coll_name => name,
        :write_concern => write_concern,
        :options => options
      ).execute(next_primary.context)
    end

    # Insert the provided documents into the collection.
    #
    # @example Insert documents into the collection.
    #   collection.insert_many([{ name: 'test' }])
    #
    # @param [ Array<Hash> ] documents The documents to insert.
    # @param [ Hash ] options The insert options.
    #
    # @return [ Result ] The database response wrapper.
    #
    # @since 2.0.0
    def insert_many(documents, options = {})
      inserts = documents.map{ |doc| { :insert_one => doc }}
      bulk_write(inserts, options)
    end

    # Execute a batch of bulk write operations.
    #
    # @example Execute a bulk write.
    #   collection.bulk_write(operations, options)
    #
    # @param [ Array<Hash> ] operations The operations.
    # @param [ Hash ] options The options.
    #
    # @return [ BulkWrite::Result ] The result of the operation.
    #
    # @since 2.0.0
    def bulk_write(operations, options = {})
      BulkWrite.get(self, operations, options).execute
    end

    # Get the fully qualified namespace of the collection.
    #
    # @example Get the fully qualified namespace.
    #   collection.namespace
    #
    # @return [ String ] The collection namespace.
    #
    # @since 2.0.0
    def namespace
      "#{database.name}.#{name}"
    end
  end
end

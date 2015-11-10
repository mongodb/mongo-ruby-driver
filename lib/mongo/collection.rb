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

require 'mongo/bulk_write'
require 'mongo/collection/view'

module Mongo

  # Represents a collection in the database and operations that can directly be
  # applied to one.
  #
  # @since 2.0.0
  class Collection
    extend Forwardable
    include Retryable

    # The capped option.
    #
    # @since 2.1.0
    CAPPED = 'capped'.freeze

    # The ns field constant.
    #
    # @since 2.1.0
    NS = 'ns'.freeze

    # @return [ Mongo::Database ] The database the collection resides in.
    attr_reader :database

    # @return [ String ] The name of the collection.
    attr_reader :name

    # @return [ Hash ] The collection options.
    attr_reader :options

    # Get client, cluster, read preference, and write concern from client.
    def_delegators :database, :client, :cluster

    # Delegate to the cluster for the next primary.
    def_delegators :cluster, :next_primary

    # Convenience delegators to find.
    def_delegators :find, :parallel_scan

    # Options that can be updated on a new Collection instance via the #with method.
    #
    # @since 2.1.0
    CHANGEABLE_OPTIONS = [ :read, :read_concern, :write ].freeze

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

    # Get the read concern for this collection instance.
    #
    # @example Get the read concern.
    #   collection.read_concern
    #
    # @return [ Hash ] The read concern.
    #
    # @since 2.2.0
    def read_concern
      @read_concern ||= options[:read_concern]
    end

    # Get the read preference on this collection.
    #
    # @example Get the read preference.
    #   collection.read_preference
    #
    # @return [ Mongo::ServerSelector ] The read preference.
    #
    # @since 2.0.0
    def read_preference
      @read_preference ||= options[:read] ? ServerSelector.get(client.options.merge(options[:read])) :
        database.read_preference
    end

    # Get the write concern on this collection.
    #
    # @example Get the write concern.
    #   collection.write_concern
    #
    # @return [ Mongo::WriteConcern ] The write concern.
    #
    # @since 2.0.0
    def write_concern
      @write_concern ||= options[:write] ? WriteConcern.get(options[:write]) :
        database.write_concern
    end

    # Provides a new collection with either a new read preference or new write concern
    # merged over the existing read preference / write concern.
    #
    # @example Get a collection with changed read preference.
    #   collection.with(:read => { :mode => :primary_preferred })
    #
    # @example Get a collection with changed write concern.
    #   collection.with(:write => { w:  3 })

    # @param [ Hash ] new_options The new options to use.
    #
    # @return [ Mongo::Collection ] A new collection instance.
    #
    # @since 2.1.0
    def with(new_options)
      new_options.keys.each do |k|
        raise Error::UnchangeableCollectionOption.new(k) unless CHANGEABLE_OPTIONS.include?(k)
      end
      Collection.new(database, name, options.merge(new_options))
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
      database.command(:collstats => name).documents[0][CAPPED]
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
    # @option options [ Hash ] :modifiers A document containing meta-operators modifying the
    #   output or behavior of a query.
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
    #   the aggregation.
    # @option options [ Integer ] :batch_size The number of documents to return per batch.
    # @option options [ Integer ] :max_time_ms The maximum amount of time in milliseconds to allow the
    #   aggregation to run.
    # @option options [ true, false ] :use_cursor Indicates whether the command will request that the server
    #   provide results using a cursor.
    # @option options [ true, false ] :bypass_document_validation Whether or
    #   not to skip document level validation.
    #
    # @return [ Aggregation ] The aggregation object.
    #
    # @since 2.1.0
    def aggregate(pipeline, options = {})
      View.new(self, {}).aggregate(pipeline, options)
    end

    # Get a count of matching documents in the collection.
    #
    # @example Get the count.
    #   collection.count(name: 1)
    #
    # @param [ Hash ] filter A filter for matching documents.
    # @param [ Hash ] options The count options.
    #
    # @option options [ Hash ] :hint The index to use.
    # @option options [ Integer ] :limit The maximum number of documents to count.
    # @option options [ Integer ] :max_time_ms The maximum amount of time to allow the command to run.
    # @option options [ Integer ] :skip The number of documents to skip before counting.
    #
    # @return [ Integer ] The document count.
    #
    # @since 2.1.0
    def count(filter = nil, options = {})
      View.new(self, filter || {}).count(options)
    end

    # Get a list of distinct values for a specific field.
    #
    # @example Get the distinct values.
    #   collection.distinct('name')
    #
    # @param [ Symbol, String ] field_name The name of the field.
    # @param [ Hash ] filter The documents from which to retrieve the distinct values.
    # @param [ Hash ] options The distinct command options.
    #
    # @option options [ Integer ] :max_time_ms The maximum amount of time to allow the command to run.
    #
    # @return [ Array<Object> ] The list of distinct values.
    #
    # @since 2.1.0
    def distinct(field_name, filter = nil, options = {})
      View.new(self, filter || {}).distinct(field_name, options)
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
      write_with_retry do
        Operation::Write::Insert.new(
          :documents => [ document ],
          :db_name => database.name,
          :coll_name => name,
          :write_concern => write_concern,
          :bypass_document_validation => !!options[:bypass_document_validation],
          :options => options,
          :id_generator => client.options[:id_generator]
        ).execute(next_primary.context)
      end
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
    # @param [ Array<Hash> ] requests The bulk write requests.
    # @param [ Hash ] options The options.
    #
    # @option options [ true, false ] :ordered Whether the operations
    #   should be executed in order.
    # @option options [ Hash ] :write_concern The write concern options.
    #   Can be :w => Integer, :fsync => Boolean, :j => Boolean.
    # @option options [ true, false ] :bypass_document_validation Whether or
    #   not to skip document level validation.
    #
    # @return [ BulkWrite::Result ] The result of the operation.
    #
    # @since 2.0.0
    def bulk_write(requests, options = {})
      BulkWrite.new(self, requests, options).execute
    end

    # Remove a document from the collection.
    #
    # @example Remove a single document from the collection.
    #   collection.delete_one
    #
    # @param [ Hash ] filter The filter to use.
    #
    # @return [ Result ] The response from the database.
    #
    # @since 2.1.0
    def delete_one(filter = nil)
      find(filter).delete_one
    end

    # Remove documents from the collection.
    #
    # @example Remove multiple documents from the collection.
    #   collection.delete_many
    #
    # @param [ Hash ] filter The filter to use.
    #
    # @return [ Result ] The response from the database.
    #
    # @since 2.1.0
    def delete_many(filter = nil)
      find(filter).delete_many
    end

    # Execute a parallel scan on the collection view.
    #
    # Returns a list of up to cursor_count cursors that can be iterated concurrently.
    # As long as the collection is not modified during scanning, each document appears once
    # in one of the cursors' result sets.
    #
    # @example Execute a parallel collection scan.
    #   collection.parallel_scan(2)
    #
    # @param [ Integer ] cursor_count The max number of cursors to return.
    #
    # @return [ Array<Cursor> ] An array of cursors.
    #
    # @since 2.1
    def parallel_scan(cursor_count)
      find.send(:parallel_scan, cursor_count)
    end

    # Replaces a single document in the collection with the new document.
    #
    # @example Replace a single document.
    #   collection.replace_one({ name: 'test' }, { name: 'test1' })
    #
    # @param [ Hash ] filter The filter to use.
    # @param [ Hash ] replacement The replacement document..
    # @param [ Hash ] options The options.
    #
    # @option options [ true, false ] :upsert Whether to upsert if the
    #   document doesn't exist.
    # @option options [ true, false ] :bypass_document_validation Whether or
    #   not to skip document level validation.
    #
    # @return [ Result ] The response from the database.
    #
    # @since 2.1.0
    def replace_one(filter, replacement, options = {})
      find(filter).replace_one(replacement, options)
    end

    # Update documents in the collection.
    #
    # @example Update multiple documents in the collection.
    #   collection.update_many({ name: 'test'}, '$set' => { name: 'test1' })
    #
    # @param [ Hash ] filter The filter to use.
    # @param [ Hash ] update The update statement.
    # @param [ Hash ] options The options.
    #
    # @option options [ true, false ] :upsert Whether to upsert if the
    #   document doesn't exist.
    # @option options [ true, false ] :bypass_document_validation Whether or
    #   not to skip document level validation.
    #
    # @return [ Result ] The response from the database.
    #
    # @since 2.1.0
    def update_many(filter, update, options = {})
      find(filter).update_many(update, options)
    end

    # Update a single document in the collection.
    #
    # @example Update a single document in the collection.
    #   collection.update_one({ name: 'test'}, '$set' => { name: 'test1'})
    #
    # @param [ Hash ] filter The filter to use.
    # @param [ Hash ] update The update statement.
    # @param [ Hash ] options The options.
    #
    # @option options [ true, false ] :upsert Whether to upsert if the
    #   document doesn't exist.
    # @option options [ true, false ] :bypass_document_validation Whether or
    #   not to skip document level validation.
    #
    # @return [ Result ] The response from the database.
    #
    # @since 2.1.0
    def update_one(filter, update, options = {})
      find(filter).update_one(update, options)
    end

    # Finds a single document in the database via findAndModify and deletes
    # it, returning the original document.
    #
    # @example Find one document and delete it.
    #   collection.find_one_and_delete(name: 'test')
    #
    # @param [ Hash ] filter The filter to use.
    # @param [ Hash ] options The options.
    #
    # @option options [ Integer ] :max_time_ms The maximum amount of time to allow the command
    #   to run in milliseconds.
    # @option options [ Hash ] :projection The fields to include or exclude in the returned doc.
    # @option options [ Hash ] :sort The key and direction pairs by which the result set
    #   will be sorted.
    # @option options [ Hash ] :write_concern The write concern options.
    #   Defaults to the collection's write concern.
    #
    # @return [ BSON::Document, nil ] The document, if found.
    #
    # @since 2.1.0
    def find_one_and_delete(filter, options = {})
      find(filter, options).find_one_and_delete
    end

    # Finds a single document via findAndModify and updates it, returning the original doc unless
    # otherwise specified.
    #
    # @example Find a document and update it, returning the original.
    #   collection.find_one_and_update({ name: 'test' }, { "$set" => { name: 'test1' }})
    #
    # @example Find a document and update it, returning the updated document.
    #   collection.find_one_and_update({ name: 'test' }, { "$set" => { name: 'test1' }}, :return_document => :after)
    #
    # @param [ Hash ] filter The filter to use.
    # @param [ BSON::Document ] update The update statement.
    # @param [ Hash ] options The options.
    #
    # @option options [ Integer ] :max_time_ms The maximum amount of time to allow the command
    #   to run in milliseconds.
    # @option options [ Hash ] :projection The fields to include or exclude in the returned doc.
    # @option options [ Hash ] :sort The key and direction pairs by which the result set
    #   will be sorted.
    # @option options [ Symbol ] :return_document Either :before or :after.
    # @option options [ true, false ] :upsert Whether to upsert if the document doesn't exist.
    # @option options [ true, false ] :bypass_document_validation Whether or
    #   not to skip document level validation.
    # @option options [ Hash ] :write_concern The write concern options.
    #   Defaults to the collection's write concern.
    #
    # @return [ BSON::Document ] The document.
    #
    # @since 2.1.0
    def find_one_and_update(filter, update, options = {})
      find(filter, options).find_one_and_update(update, options)
    end

    # Finds a single document and replaces it, returning the original doc unless
    # otherwise specified.
    #
    # @example Find a document and replace it, returning the original.
    #   collection.find_one_and_replace({ name: 'test' }, { name: 'test1' })
    #
    # @example Find a document and replace it, returning the new document.
    #   collection.find_one_and_replace({ name: 'test' }, { name: 'test1' }, :return_document => :after)
    #
    # @param [ Hash ] filter The filter to use.
    # @param [ BSON::Document ] replacement The replacement document.
    # @param [ Hash ] options The options.
    #
    # @option options [ Integer ] :max_time_ms The maximum amount of time to allow the command
    #   to run in milliseconds.
    # @option options [ Hash ] :projection The fields to include or exclude in the returned doc.
    # @option options [ Hash ] :sort The key and direction pairs by which the result set
    #   will be sorted.
    # @option options [ Symbol ] :return_document Either :before or :after.
    # @option options [ true, false ] :upsert Whether to upsert if the document doesn't exist.
    # @option options [ true, false ] :bypass_document_validation Whether or
    #   not to skip document level validation.
    # @option options [ Hash ] :write_concern The write concern options.
    #   Defaults to the collection's write concern.
    #
    # @return [ BSON::Document ] The document.
    #
    # @since 2.1.0
    def find_one_and_replace(filter, replacement, options = {})
      find(filter, options).find_one_and_update(replacement, options)
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

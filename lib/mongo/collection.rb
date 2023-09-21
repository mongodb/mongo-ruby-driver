# frozen_string_literal: true
# rubocop:todo all

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

require 'mongo/bulk_write'
require 'mongo/collection/view'
require 'mongo/collection/helpers'
require 'mongo/collection/queryable_encryption'

module Mongo

  # Represents a collection in the database and operations that can directly be
  # applied to one.
  #
  # @since 2.0.0
  class Collection
    extend Forwardable
    include Retryable
    include QueryableEncryption
    include Helpers

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

    # Get client, cluster, read preference, write concern, and encrypted_fields_map from client.
    def_delegators :database, :client, :cluster, :encrypted_fields_map

    # Delegate to the cluster for the next primary.
    def_delegators :cluster, :next_primary

    # Options that can be updated on a new Collection instance via the #with method.
    #
    # @since 2.1.0
    CHANGEABLE_OPTIONS = [ :read, :read_concern, :write, :write_concern ].freeze

    # Options map to transform create collection options.
    #
    # @api private
    CREATE_COLLECTION_OPTIONS = {
      :time_series => :timeseries,
      :expire_after => :expireAfterSeconds,
      :clustered_index => :clusteredIndex,
      :change_stream_pre_and_post_images => :changeStreamPreAndPostImages,
      :encrypted_fields => :encryptedFields,
      :validator => :validator,
      :view_on => :viewOn
    }

    # Check if a collection is equal to another object. Will check the name and
    # the database for equality.
    #
    # @example Check collection equality.
    #   collection == other
    #
    # @param [ Object ] other The object to check.
    #
    # @return [ true | false ] If the objects are equal.
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
    # @option opts [ true | false ] :capped Create a fixed-sized collection.
    # @option opts [ Hash ] :change_stream_pre_and_post_images Used to enable
    #   pre- and post-images on the created collection.
    #   The hash may have the following items:
    #   - *:enabled* -- true or false.
    # @option opts [ Hash ] :clustered_index Create a clustered index.
    #   This option specifies how this collection should be clustered on _id.
    #   The hash may have the following items:
    #   - *:key* -- The clustered index key field. Must be set to { _id: 1 }.
    #   - *:unique* -- Must be set to true. The collection will not accept
    #     inserted or updated documents where the clustered index key value
    #     matches an existing value in the index.
    #   - *:name* -- Optional. A name that uniquely identifies the clustered index.
    # @option opts [ Hash ] :collation The collation to use.
    # @option opts [ Hash ] :encrypted_fields Hash describing encrypted fields
    #   for queryable encryption.
    # @option opts [ Integer ] :expire_after Number indicating
    #   after how many seconds old time-series data should be deleted.
    # @option opts [ Integer ] :max The maximum number of documents in a
    #   capped collection. The size limit takes precedents over max.
    # @option opts [ Array<Hash> ] :pipeline An array of pipeline stages.
    #   A view will be created by applying this pipeline to the view_on
    #   collection or view.
    # @option options [ Hash ] :read_concern The read concern options hash,
    #   with the following optional keys:
    #   - *:level* -- the read preference level as a symbol; valid values
    #      are *:local*, *:majority*, and *:snapshot*
    # @option options [ Hash ] :read The read preference options.
    #   The hash may have the following items:
    #   - *:mode* -- read preference specified as a symbol; valid values are
    #     *:primary*, *:primary_preferred*, *:secondary*, *:secondary_preferred*
    #     and *:nearest*.
    #   - *:tag_sets* -- an array of hashes.
    #   - *:local_threshold*.
    # @option opts [ Session ] :session The session to use for the operation.
    # @option opts [ Integer ] :size The size of the capped collection.
    # @option opts [ Hash ] :time_series Create a time-series collection.
    #   The hash may have the following items:
    #   - *:timeField* -- The name of the field which contains the date in each
    #     time series document.
    #   - *:metaField* -- The name of the field which contains metadata in each
    #     time series document.
    #   - *:granularity* -- Set the granularity to the value that is the closest
    #     match to the time span between consecutive incoming measurements.
    #     Possible values are "seconds" (default), "minutes", and "hours".
    # @option opts [ Hash ] :validator Hash describing document validation
    #   options for the collection.
    # @option opts [ String ] :view_on The name of the source collection or
    #   view from which to create a view.
    # @option opts [ Hash ] :write Deprecated. Equivalent to :write_concern
    #   option.
    # @option opts [ Hash ] :write_concern The write concern options.
    #   Can be :w => Integer|String, :fsync => Boolean, :j => Boolean.
    #
    # @since 2.0.0
    def initialize(database, name, options = {})
      raise Error::InvalidCollectionName.new unless name
      if options[:write] && options[:write_concern] && options[:write] != options[:write_concern]
        raise ArgumentError, "If :write and :write_concern are both given, they must be identical: #{options.inspect}"
      end
      @database = database
      @name = name.to_s.freeze
      @options = options.dup
=begin WriteConcern object support
      if @options[:write_concern].is_a?(WriteConcern::Base)
        # Cache the instance so that we do not needlessly reconstruct it.
        @write_concern = @options[:write_concern]
        @options[:write_concern] = @write_concern.options
      end
=end
      @options.freeze
    end

    # Get the effective read concern for this collection instance.
    #
    # If a read concern was provided in collection options, that read concern
    # will be returned, otherwise the database's effective read concern will
    # be returned.
    #
    # @example Get the read concern.
    #   collection.read_concern
    #
    # @return [ Hash ] The read concern.
    #
    # @since 2.2.0
    def read_concern
      options[:read_concern] || database.read_concern
    end

    # Get the server selector for this collection.
    #
    # @example Get the server selector.
    #   collection.server_selector
    #
    # @return [ Mongo::ServerSelector ] The server selector.
    #
    # @since 2.0.0
    def server_selector
      @server_selector ||= ServerSelector.get(read_preference || database.server_selector)
    end

    # Get the effective read preference for this collection.
    #
    # If a read preference was provided in collection options, that read
    # preference will be returned, otherwise the database's effective read
    # preference will be returned.
    #
    # @example Get the read preference.
    #   collection.read_preference
    #
    # @return [ Hash ] The read preference.
    #
    # @since 2.0.0
    def read_preference
      @read_preference ||= options[:read] || database.read_preference
    end

    # Get the effective write concern on this collection.
    #
    # If a write concern was provided in collection options, that write
    # concern will be returned, otherwise the database's effective write
    # concern will be returned.
    #
    # @example Get the write concern.
    #   collection.write_concern
    #
    # @return [ Mongo::WriteConcern ] The write concern.
    #
    # @since 2.0.0
    def write_concern
      @write_concern ||= WriteConcern.get(
        options[:write_concern] || options[:write] || database.write_concern)
    end

    # Get the write concern to use for an operation on this collection,
    # given a session.
    #
    # If the session is in a transaction and the collection
    # has an unacknowledged write concern, remove the write
    # concern's :w option. Otherwise, return the unmodified
    # write concern.
    #
    # @return [ Mongo::WriteConcern ] The write concern.
    #
    # @api private
    def write_concern_with_session(session)
      wc = write_concern
      if session && session.in_transaction?
        if wc && !wc.acknowledged?
          opts = wc.options.dup
          opts.delete(:w)
          return WriteConcern.get(opts)
        end
      end
      wc
    end

    # Provides a new collection with either a new read preference, new read
    # concern or new write concern merged over the existing read preference /
    # read concern / write concern.
    #
    # @example Get a collection with a changed read preference.
    #   collection.with(read: { mode: :primary_preferred })

    # @example Get a collection with a changed read concern.
    #   collection.with(read_concern: { level: :majority })
    #
    # @example Get a collection with a changed write concern.
    #   collection.with(write_concern: { w:  3 })
    #
    # @param [ Hash ] new_options The new options to use.
    #
    # @option new_options [ Hash ] :read The read preference options.
    #   The hash may have the following items:
    #   - *:mode* -- read preference specified as a symbol; valid values are
    #     *:primary*, *:primary_preferred*, *:secondary*, *:secondary_preferred*
    #     and *:nearest*.
    #   - *:tag_sets* -- an array of hashes.
    #   - *:local_threshold*.
    # @option new_options [ Hash ] :read_concern The read concern options hash,
    #   with the following optional keys:
    #   - *:level* -- the read preference level as a symbol; valid values
    #      are *:local*, *:majority*, and *:snapshot*
    # @option new_options [ Hash ] :write Deprecated. Equivalent to :write_concern
    #   option.
    # @option new_options [ Hash ] :write_concern The write concern options.
    #   Can be :w => Integer|String, :fsync => Boolean, :j => Boolean.
    #
    # @return [ Mongo::Collection ] A new collection instance.
    #
    # @since 2.1.0
    def with(new_options)
      new_options.keys.each do |k|
        raise Error::UnchangeableCollectionOption.new(k) unless CHANGEABLE_OPTIONS.include?(k)
      end
      options = @options.dup
      if options[:write] && new_options[:write_concern]
        options.delete(:write)
      end
      if options[:write_concern] && new_options[:write]
        options.delete(:write_concern)
      end
      Collection.new(database, name, options.update(new_options))
    end

    # Is the collection capped?
    #
    # @example Is the collection capped?
    #   collection.capped?
    #
    # @return [ true | false ] If the collection is capped.
    #
    # @since 2.0.0
    def capped?
      database.list_collections(filter: { name: name })
        .first
        &.dig('options', CAPPED) || false
    end

    # Force the collection to be created in the database.
    #
    # @example Force the collection to be created.
    #   collection.create
    #
    # @param [ Hash ] opts The options for the create operation.
    #
    # @option opts [ true | false ] :capped Create a fixed-sized collection.
    # @option opts [ Hash ] :change_stream_pre_and_post_images Used to enable
    #   pre- and post-images on the created collection.
    #   The hash may have the following items:
    #   - *:enabled* -- true or false.
    # @option opts [ Hash ] :clustered_index Create a clustered index.
    #   This option specifies how this collection should be clustered on _id.
    #   The hash may have the following items:
    #   - *:key* -- The clustered index key field. Must be set to { _id: 1 }.
    #   - *:unique* -- Must be set to true. The collection will not accept
    #     inserted or updated documents where the clustered index key value
    #     matches an existing value in the index.
    #   - *:name* -- Optional. A name that uniquely identifies the clustered index.
    # @option opts [ Hash ] :collation The collation to use.
    # @option opts [ Hash ] :encrypted_fields Hash describing encrypted fields
    #   for queryable encryption.
    # @option opts [ Integer ] :expire_after Number indicating
    #   after how many seconds old time-series data should be deleted.
    # @option opts [ Integer ] :max The maximum number of documents in a
    #   capped collection. The size limit takes precedents over max.
    # @option opts [ Array<Hash> ] :pipeline An array of pipeline stages.
    #   A view will be created by applying this pipeline to the view_on
    #   collection or view.
    # @option opts [ Session ] :session The session to use for the operation.
    # @option opts [ Integer ] :size The size of the capped collection.
    # @option opts [ Hash ] :time_series Create a time-series collection.
    #   The hash may have the following items:
    #   - *:timeField* -- The name of the field which contains the date in each
    #     time series document.
    #   - *:metaField* -- The name of the field which contains metadata in each
    #     time series document.
    #   - *:granularity* -- Set the granularity to the value that is the closest
    #     match to the time span between consecutive incoming measurements.
    #     Possible values are "seconds" (default), "minutes", and "hours".
    # @option opts [ Hash ] :validator Hash describing document validation
    #   options for the collection.
    # @option opts [ String ] :view_on The name of the source collection or
    #   view from which to create a view.
    # @option opts [ Hash ] :write Deprecated. Equivalent to :write_concern
    #   option.
    # @option opts [ Hash ] :write_concern The write concern options.
    #   Can be :w => Integer|String, :fsync => Boolean, :j => Boolean.
    #
    # @return [ Result ] The result of the command.
    #
    # @since 2.0.0
    def create(opts = {})
      # Passing read options to create command causes it to break.
      # Filter the read options out. Session is also excluded here as it gets
      # used by the call to with_session and should not be part of the
      # operation. If it gets passed to the operation it would fail BSON
      # serialization.
      # TODO put the list of read options in a class-level constant when
      # we figure out what the full set of them is.
      options = Hash[self.options.merge(opts).reject do |key, value|
        %w(read read_preference read_concern session).include?(key.to_s)
      end]
      # Converting Ruby options to server style.
      CREATE_COLLECTION_OPTIONS.each do |ruby_key, server_key|
        if options.key?(ruby_key)
          options[server_key] = options.delete(ruby_key)
        end
      end
      operation = { :create => name }.merge(options)
      operation.delete(:write)
      operation.delete(:write_concern)
      client.send(:with_session, opts) do |session|
        write_concern = if opts[:write_concern]
          WriteConcern.get(opts[:write_concern])
        else
          self.write_concern
        end

        context = Operation::Context.new(client: client, session: session)
        maybe_create_qe_collections(opts[:encrypted_fields], client, session) do |encrypted_fields|
          Operation::Create.new(
            selector: operation,
            db_name: database.name,
            write_concern: write_concern,
            session: session,
            # Note that these are collection options, collation isn't
            # taken from options passed to the create method.
            collation: options[:collation] || options['collation'],
            encrypted_fields: encrypted_fields,
            validator: options[:validator],
          ).execute(next_primary(nil, session), context: context)
        end
      end
    end

    # Drop the collection. Will also drop all indexes associated with the
    # collection, as well as associated queryable encryption collections.
    #
    # @note An error returned if the collection doesn't exist is suppressed.
    #
    # @example Drop the collection.
    #   collection.drop
    #
    # @param [ Hash ] opts The options for the drop operation.
    #
    # @option opts [ Session ] :session The session to use for the operation.
    # @option opts [ Hash ] :write_concern The write concern options.
    # @option opts [ Hash | nil ] :encrypted_fields Encrypted fields hash that
    #   was provided to `create` collection helper.
    #
    # @return [ Result ] The result of the command.
    #
    # @since 2.0.0
    def drop(opts = {})
      client.send(:with_session, opts) do |session|
        maybe_drop_emm_collections(opts[:encrypted_fields], client, session) do
          temp_write_concern = write_concern
          write_concern = if opts[:write_concern]
            WriteConcern.get(opts[:write_concern])
          else
            temp_write_concern
          end
          context = Operation::Context.new(client: client, session: session)
          operation = Operation::Drop.new({
            selector: { :drop => name },
            db_name: database.name,
            write_concern: write_concern,
            session: session,
          })
          do_drop(operation, session, context)
        end
      end
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
    # @option options [ true | false ] :allow_disk_use When set to true, the
    #   server can write temporary data to disk while executing the find
    #   operation. This option is only available on MongoDB server versions
    #   4.4 and newer.
    # @option options [ true | false ] :allow_partial_results Allows the query to get partial
    #   results if some shards are down.
    # @option options [ Integer ] :batch_size The number of documents returned in each batch
    #   of results from MongoDB.
    # @option options [ Hash ] :collation The collation to use.
    # @option options [ Object ] :comment A user-provided comment to attach to
    #   this command.
    # @option options [ :tailable, :tailable_await ] :cursor_type The type of cursor to use.
    # @option options [ Integer ] :limit The max number of docs to return from the query.
    # @option options [ Integer ] :max_time_ms
    #   The maximum amount of time to allow the query to run, in milliseconds.
    # @option options [ Hash ] :modifiers A document containing meta-operators modifying the
    #   output or behavior of a query.
    # @option options [ true | false ] :no_cursor_timeout The server normally times out idle
    #   cursors after an inactivity period (10 minutes) to prevent excess memory use.
    #   Set this option to prevent that.
    # @option options [ true | false ] :oplog_replay For internal replication
    #   use only, applications should not set this option.
    # @option options [ Hash ] :projection The fields to include or exclude from each doc
    #   in the result set.
    # @option options [ Session ] :session The session to use.
    # @option options [ Integer ] :skip The number of docs to skip before returning results.
    # @option options [ Hash ] :sort The key and direction pairs by which the result set
    #   will be sorted.
    # @option options [ Hash ] :let Mapping of variables to use in the command.
    #   See the server documentation for details.
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
    # @option options [ true | false ] :allow_disk_use Set to true if disk
    #   usage is allowed during the aggregation.
    # @option options [ Integer ] :batch_size The number of documents to return
    #   per batch.
    # @option options [ true | false ] :bypass_document_validation Whether or
    #   not to skip document level validation.
    # @option options [ Hash ] :collation The collation to use.
    # @option options [ Object ] :comment A user-provided
    #   comment to attach to this command.
    # @option options [ String ] :hint The index to use for the aggregation.
    # @option options [ Hash ] :let Mapping of variables to use in the pipeline.
    #   See the server documentation for details.
    # @option options [ Integer ] :max_time_ms The maximum amount of time in
    #   milliseconds to allow the aggregation to run.
    # @option options [ true | false ] :use_cursor Indicates whether the command
    #   will request that the server provide results using a cursor. Note that
    #   as of server version 3.6, aggregations always provide results using a
    #   cursor and this option is therefore not valid.
    # @option options [ Session ] :session The session to use.
    #
    # @return [ View::Aggregation ] The aggregation object.
    #
    # @since 2.1.0
    def aggregate(pipeline, options = {})
      View.new(self, {}, options).aggregate(pipeline, options)
    end

    # As of version 3.6 of the MongoDB server, a ``$changeStream`` pipeline
    # stage is supported in the aggregation framework. This stage allows users
    # to request that notifications are sent for all changes to a particular
    # collection.
    #
    # @example Get change notifications for a given collection.
    #   collection.watch([{ '$match' => { operationType: { '$in' => ['insert', 'replace'] } } }])
    #
    # @param [ Array<Hash> ] pipeline Optional additional filter operators.
    # @param [ Hash ] options The change stream options.
    #
    # @option options [ String ] :full_document Allowed values: nil, 'default',
    #   'updateLookup', 'whenAvailable', 'required'.
    #
    #   The default is to not send a value (i.e. nil), which is equivalent to
    #   'default'. By default, the change notification for partial updates will
    #   include a delta describing the changes to the document.
    #
    #   When set to 'updateLookup', the change notification for partial updates
    #   will include both a delta describing the changes to the document as well
    #   as a copy of the entire document that was changed from some time after
    #   the change occurred.
    #
    #   When set to 'whenAvailable', configures the change stream to return the
    #   post-image of the modified document for replace and update change events
    #   if the post-image for this event is available.
    #
    #   When set to 'required', the same behavior as 'whenAvailable' except that
    #   an error is raised if the post-image is not available.
    # @option options [ String ] :full_document_before_change Allowed values: nil,
    #   'whenAvailable', 'required', 'off'.
    #
    #   The default is to not send a value (i.e. nil), which is equivalent to 'off'.
    #
    #   When set to 'whenAvailable', configures the change stream to return the
    #   pre-image of the modified document for replace, update, and delete change
    #   events if it is available.
    #
    #   When set to 'required', the same behavior as 'whenAvailable' except that
    #   an error is raised if the pre-image is not available.
    # @option options [ BSON::Document, Hash ] :resume_after Specifies the
    #   logical starting point for the new change stream.
    # @option options [ Integer ] :max_await_time_ms The maximum amount of time
    #   for the server to wait on new documents to satisfy a change stream query.
    # @option options [ Integer ] :batch_size The number of documents to return
    #   per batch.
    # @option options [ BSON::Document, Hash ] :collation The collation to use.
    # @option options [ Session ] :session The session to use.
    # @option options [ BSON::Timestamp ] :start_at_operation_time Only return
    #   changes that occurred at or after the specified timestamp. Any command run
    #   against the server will return a cluster time that can be used here.
    #   Only recognized by server versions 4.0+.
    # @option options [ Object ] :comment A user-provided
    #   comment to attach to this command.
    # @option options [ Boolean ] :show_expanded_events Enables the server to
    #   send the 'expanded' list of change stream events. The list of additional
    #   events included with this flag set are: createIndexes, dropIndexes,
    #   modify, create, shardCollection, reshardCollection,
    #   refineCollectionShardKey.
    #
    # @note A change stream only allows 'majority' read concern.
    # @note This helper method is preferable to running a raw aggregation with
    #   a $changeStream stage, for the purpose of supporting resumability.
    #
    # @return [ ChangeStream ] The change stream object.
    #
    # @since 2.5.0
    def watch(pipeline = [], options = {})
      view_options = options.dup
      view_options[:await_data] = true if options[:max_await_time_ms]
      View::ChangeStream.new(View.new(self, {}, view_options), pipeline, nil, options)
    end

    # Gets an estimated number of matching documents in the collection.
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
    # @option options [ Hash ] :read The read preference options.
    # @option options [ Hash ] :collation The collation to use.
    # @option options [ Session ] :session The session to use.
    # @option options [ Object ] :comment A user-provided
    #   comment to attach to this command.
    #
    # @return [ Integer ] The document count.
    #
    # @since 2.1.0
    #
    # @deprecated Use #count_documents or estimated_document_count instead. However, note that the
    #   following operators will need to be substituted when switching to #count_documents:
    #     * $where should be replaced with $expr (only works on 3.6+)
    #     * $near should be replaced with $geoWithin with $center
    #     * $nearSphere should be replaced with $geoWithin with $centerSphere
    def count(filter = nil, options = {})
      View.new(self, filter || {}, options).count(options)
    end

    # Gets the number of documents matching the query. Unlike the deprecated
    # #count method, this will return the exact number of documents matching
    # the filter (or exact number of documents in the collection, if no filter
    # is provided) rather than an estimate.
    #
    # Use #estimated_document_count to retrieve an estimate of the number
    # of documents in the collection using the collection metadata.
    #
    # @param [ Hash ] filter A filter for matching documents.
    # @param [ Hash ] options Options for the operation.
    #
    # @option options :skip [ Integer ] The number of documents to skip.
    # @option options :hint [ Hash ] Override default index selection and force
    #   MongoDB to use a specific index for the query. Requires server version 3.6+.
    # @option options :limit [ Integer ] Max number of docs to count.
    # @option options :max_time_ms [ Integer ] The maximum amount of time to allow the
    #   command to run.
    # @option options :read [ Hash ] The read preference options.
    # @option options :collation [ Hash ] The collation to use.
    # @option options [ Session ] :session The session to use.
    # @option options [ Object ] :comment A user-provided
    #   comment to attach to this command.
    #
    # @return [ Integer ] The document count.
    #
    # @since 2.6.0
    def count_documents(filter = {}, options = {})
      View.new(self, filter, options).count_documents(options)
    end

    # Gets an estimate of the number of documents in the collection using the
    # collection metadata.
    #
    # Use #count_documents to retrieve the exact number of documents in the
    # collection, or to count documents matching a filter.
    #
    # @param [ Hash ] options Options for the operation.
    #
    # @option options :max_time_ms [ Integer ] The maximum amount of time to allow
    #   the command to run for on the server.
    # @option options [ Hash ] :read The read preference options.
    # @option options [ Object ] :comment A user-provided
    #   comment to attach to this command.
    #
    # @return [ Integer ] The document count.
    #
    # @since 2.6.0
    def estimated_document_count(options = {})
      View.new(self, {}, options).estimated_document_count(options)
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
    # @option options [ Hash ] :read The read preference options.
    # @option options [ Hash ] :collation The collation to use.
    # @option options [ Session ] :session The session to use.
    #
    # @return [ Array<Object> ] The list of distinct values.
    #
    # @since 2.1.0
    def distinct(field_name, filter = nil, options = {})
      View.new(self, filter || {}, options).distinct(field_name, options)
    end

    # Get a view of all indexes for this collection. Can be iterated or has
    # more operations.
    #
    # @example Get the index view.
    #   collection.indexes
    #
    # @param [ Hash ] options Options for getting a list of all indexes.
    #
    # @option options [ Session ] :session The session to use.
    #
    # @return [ Index::View ] The index view.
    #
    # @since 2.0.0
    def indexes(options = {})
      Index::View.new(self, options)
    end

    # Get a view of all search indexes for this collection. Can be iterated or
    # operated on directly. If id or name are given, the iterator will return
    # only the indicated index. For all other operations, id and name are
    # ignored.
    #
    # @note Only one of id or name may be given; it is an error to specify both,
    #   although both may be omitted safely.
    #
    # @param [ Hash ] options The options to use to configure the view.
    #
    # @option options [ String ] :id The id of the specific index to query (optional)
    # @option options [ String ] :name The name of the specific index to query (optional)
    # @option options [ Hash ] :aggregate The options hash to pass to the
    #    aggregate command (optional)
    #
    # @return [ SearchIndex::View ] The search index view.
    #
    # @since 2.0.0
    def search_indexes(options = {})
      SearchIndex::View.new(self, options)
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
    # @param [ Hash ] opts The insert options.
    #
    # @option opts [ true | false ] :bypass_document_validation Whether or
    #   not to skip document level validation.
    # @option opts [ Object ] :comment A user-provided comment to attach to
    #   this command.
    # @option opts [ Session ] :session The session to use for the operation.
    # @option opts [ Hash ] :write_concern The write concern options.
    #   Can be :w => Integer, :fsync => Boolean, :j => Boolean.
    #
    # @return [ Result ] The database response wrapper.
    #
    # @since 2.0.0
    def insert_one(document, opts = {})
      QueryCache.clear_namespace(namespace)

      client.send(:with_session, opts) do |session|
        write_concern = if opts[:write_concern]
          WriteConcern.get(opts[:write_concern])
        else
          write_concern_with_session(session)
        end

        if document.nil?
          raise ArgumentError, "Document to be inserted cannot be nil"
        end

        context = Operation::Context.new(client: client, session: session)
        write_with_retry(write_concern, context: context) do |connection, txn_num, context|
          Operation::Insert.new(
            :documents => [ document ],
            :db_name => database.name,
            :coll_name => name,
            :write_concern => write_concern,
            :bypass_document_validation => !!opts[:bypass_document_validation],
            :options => opts,
            :id_generator => client.options[:id_generator],
            :session => session,
            :txn_num => txn_num,
            :comment => opts[:comment]
          ).execute_with_connection(connection, context: context)
        end
      end
    end

    # Insert the provided documents into the collection.
    #
    # @example Insert documents into the collection.
    #   collection.insert_many([{ name: 'test' }])
    #
    # @param [ Enumerable<Hash> ] documents The documents to insert.
    # @param [ Hash ] options The insert options.
    #
    # @option options [ true | false ] :bypass_document_validation Whether or
    #   not to skip document level validation.
    # @option options [ Object ] :comment A user-provided comment to attach to
    #   this command.
    # @option options [ true | false ] :ordered Whether the operations
    #   should be executed in order.
    # @option options [ Session ] :session The session to use for the operation.
    # @option options [ Hash ] :write_concern The write concern options.
    #   Can be :w => Integer, :fsync => Boolean, :j => Boolean.
    #
    # @return [ Result ] The database response wrapper.
    #
    # @since 2.0.0
    def insert_many(documents, options = {})
      QueryCache.clear_namespace(namespace)

      inserts = documents.map{ |doc| { :insert_one => doc }}
      bulk_write(inserts, options)
    end

    # Execute a batch of bulk write operations.
    #
    # @example Execute a bulk write.
    #   collection.bulk_write(operations, options)
    #
    # @param [ Enumerable<Hash> ] requests The bulk write requests.
    # @param [ Hash ] options The options.
    #
    # @option options [ true | false ] :ordered Whether the operations
    #   should be executed in order.
    # @option options [ Hash ] :write_concern The write concern options.
    #   Can be :w => Integer, :fsync => Boolean, :j => Boolean.
    # @option options [ true | false ] :bypass_document_validation Whether or
    #   not to skip document level validation.
    # @option options [ Session ] :session The session to use for the set of operations.
    # @option options [ Hash ] :let Mapping of variables to use in the command.
    #   See the server documentation for details.
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
    # @param [ Hash ] options The options.
    #
    # @option options [ Hash ] :collation The collation to use.
    # @option options [ Session ] :session The session to use.
    # @option options [ Hash | String ] :hint The index to use for this operation.
    #   May be specified as a Hash (e.g. { _id: 1 }) or a String (e.g. "_id_").
    # @option options [ Hash ] :let Mapping of variables to use in the command.
    #   See the server documentation for details.
    #
    # @return [ Result ] The response from the database.
    #
    # @since 2.1.0
    def delete_one(filter = nil, options = {})
      find(filter, options).delete_one(options)
    end

    # Remove documents from the collection.
    #
    # @example Remove multiple documents from the collection.
    #   collection.delete_many
    #
    # @param [ Hash ] filter The filter to use.
    # @param [ Hash ] options The options.
    #
    # @option options [ Hash ] :collation The collation to use.
    # @option options [ Session ] :session The session to use.
    # @option options [ Hash | String ] :hint The index to use for this operation.
    #   May be specified as a Hash (e.g. { _id: 1 }) or a String (e.g. "_id_").
    # @option options [ Hash ] :let Mapping of variables to use in the command.
    #   See the server documentation for details.
    #
    # @return [ Result ] The response from the database.
    #
    # @since 2.1.0
    def delete_many(filter = nil, options = {})
      find(filter, options).delete_many(options)
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
    # @param [ Hash ] options The parallel scan command options.
    #
    # @option options [ Integer ] :max_time_ms The maximum amount of time to allow the command
    #   to run in milliseconds.
    # @option options [ Session ] :session The session to use.
    #
    # @return [ Array<Cursor> ] An array of cursors.
    #
    # @since 2.1
    def parallel_scan(cursor_count, options = {})
      find({}, options).send(:parallel_scan, cursor_count, options)
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
    # @option options [ true | false ] :upsert Whether to upsert if the
    #   document doesn't exist.
    # @option options [ true | false ] :bypass_document_validation Whether or
    #   not to skip document level validation.
    # @option options [ Hash ] :collation The collation to use.
    # @option options [ Session ] :session The session to use.
    # @option options [ Hash | String ] :hint The index to use for this operation.
    #   May be specified as a Hash (e.g. { _id: 1 }) or a String (e.g. "_id_").
    # @option options [ Hash ] :let Mapping of variables to use in the command.
    #   See the server documentation for details.
    #
    # @return [ Result ] The response from the database.
    #
    # @since 2.1.0
    def replace_one(filter, replacement, options = {})
      find(filter, options).replace_one(replacement, options)
    end

    # Update documents in the collection.
    #
    # @example Update multiple documents in the collection.
    #   collection.update_many({ name: 'test'}, '$set' => { name: 'test1' })
    #
    # @param [ Hash ] filter The filter to use.
    # @param [ Hash | Array<Hash> ] update The update document or pipeline.
    # @param [ Hash ] options The options.
    #
    # @option options [ true | false ] :upsert Whether to upsert if the
    #   document doesn't exist.
    # @option options [ true | false ] :bypass_document_validation Whether or
    #   not to skip document level validation.
    # @option options [ Hash ] :collation The collation to use.
    # @option options [ Array ] :array_filters A set of filters specifying to which array elements
    #   an update should apply.
    # @option options [ Session ] :session The session to use.
    # @option options [ Hash | String ] :hint The index to use for this operation.
    #   May be specified as a Hash (e.g. { _id: 1 }) or a String (e.g. "_id_").
    # @option options [ Hash ] :let Mapping of variables to use in the command.
    #   See the server documentation for details.
    #
    # @return [ Result ] The response from the database.
    #
    # @since 2.1.0
    def update_many(filter, update, options = {})
      find(filter, options).update_many(update, options)
    end

    # Update a single document in the collection.
    #
    # @example Update a single document in the collection.
    #   collection.update_one({ name: 'test'}, '$set' => { name: 'test1'})
    #
    # @param [ Hash ] filter The filter to use.
    # @param [ Hash | Array<Hash> ] update The update document or pipeline.
    # @param [ Hash ] options The options.
    #
    # @option options [ true | false ] :upsert Whether to upsert if the
    #   document doesn't exist.
    # @option options [ true | false ] :bypass_document_validation Whether or
    #   not to skip document level validation.
    # @option options [ Hash ] :collation The collation to use.
    # @option options [ Array ] :array_filters A set of filters specifying to which array elements
    #   an update should apply.
    # @option options [ Session ] :session The session to use.
    # @option options [ Hash | String ] :hint The index to use for this operation.
    #   May be specified as a Hash (e.g. { _id: 1 }) or a String (e.g. "_id_").
    # @option options [ Hash ] :let Mapping of variables to use in the command.
    #   See the server documentation for details.
    #
    # @return [ Result ] The response from the database.
    #
    # @since 2.1.0
    def update_one(filter, update, options = {})
      find(filter, options).update_one(update, options)
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
    # @option options [ Hash ] :collation The collation to use.
    # @option options [ Session ] :session The session to use.
    # @option options [ Hash | String ] :hint The index to use for this operation.
    #   May be specified as a Hash (e.g. { _id: 1 }) or a String (e.g. "_id_").
    # @option options [ Hash ] :let Mapping of variables to use in the command.
    #   See the server documentation for details.
    #
    # @return [ BSON::Document, nil ] The document, if found.
    #
    # @since 2.1.0
    def find_one_and_delete(filter, options = {})
      find(filter, options).find_one_and_delete(options)
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
    # @param [ Hash | Array<Hash> ] update The update document or pipeline.
    # @param [ Hash ] options The options.
    #
    # @option options [ Integer ] :max_time_ms The maximum amount of time to allow the command
    #   to run in milliseconds.
    # @option options [ Hash ] :projection The fields to include or exclude in the returned doc.
    # @option options [ Hash ] :sort The key and direction pairs by which the result set
    #   will be sorted.
    # @option options [ Symbol ] :return_document Either :before or :after.
    # @option options [ true | false ] :upsert Whether to upsert if the document doesn't exist.
    # @option options [ true | false ] :bypass_document_validation Whether or
    #   not to skip document level validation.
    # @option options [ Hash ] :write_concern The write concern options.
    #   Defaults to the collection's write concern.
    # @option options [ Hash ] :collation The collation to use.
    # @option options [ Array ] :array_filters A set of filters specifying to which array elements
    #   an update should apply.
    # @option options [ Session ] :session The session to use.
    # @option options [ Hash | String ] :hint The index to use for this operation.
    #   May be specified as a Hash (e.g. { _id: 1 }) or a String (e.g. "_id_").
    # @option options [ Hash ] :let Mapping of variables to use in the command.
    #   See the server documentation for details.
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
    # @option options [ true | false ] :upsert Whether to upsert if the document doesn't exist.
    # @option options [ true | false ] :bypass_document_validation Whether or
    #   not to skip document level validation.
    # @option options [ Hash ] :write_concern The write concern options.
    #   Defaults to the collection's write concern.
    # @option options [ Hash ] :collation The collation to use.
    # @option options [ Session ] :session The session to use.
    # @option options [ Hash | String ] :hint The index to use for this operation.
    #   May be specified as a Hash (e.g. { _id: 1 }) or a String (e.g. "_id_").
    # @option options [ Hash ] :let Mapping of variables to use in the command.
    #   See the server documentation for details.
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

    # Whether the collection is a system collection.
    #
    # @return [ Boolean ] Whether the system is a system collection.
    #
    # @api private
    def system_collection?
      name.start_with?('system.')
    end
  end
end

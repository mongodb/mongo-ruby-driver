# Copyright (C) 2009-2013 MongoDB, Inc.
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

module Mongo

  # A named collection of documents in a database.
  class Collection
    include Mongo::Logging
    include Mongo::WriteConcern

    attr_reader :db,
                :name,
                :pk_factory,
                :hint,
                :write_concern,
                :capped,
                :operation_writer,
                :command_writer

    # Read Preference
    attr_accessor :read,
                  :tag_sets,
                  :acceptable_latency

    # Initialize a collection object.
    #
    # @param [String, Symbol] name the name of the collection.
    # @param [DB] db a MongoDB database instance.
    #
    # @option opts [String, Integer, Symbol] :w (1) Set default number of nodes to which a write
    #   should be acknowledged.
    # @option opts [Integer] :wtimeout (nil) Set replica set acknowledgement timeout.
    # @option opts [Boolean] :j (false) If true, block until write operations have been committed
    #   to the journal. Cannot be used in combination with 'fsync'. Prior to MongoDB 2.6 this option was
    #   ignored if the server was running without journaling. Starting with MongoDB 2.6, write operations will
    #   fail with an exception if this option is used when the server is running without journaling.
    # @option opts [Boolean] :fsync (false) If true, and the server is running without journaling, blocks until
    #   the server has synced all data files to disk. If the server is running with journaling, this acts the same as
    #   the 'j' option, blocking until write operations have been committed to the journal.
    #   Cannot be used in combination with 'j'.
    #
    #   Notes about write concern:
    #     These write concern options will be used for insert, update, and remove methods called on this
    #     Collection instance. If no value is provided, the default values set on this instance's DB will be used.
    #     These option values can be overridden for any invocation of insert, update, or remove.
    #
    # @option opts [:create_pk] :pk (BSON::ObjectId) A primary key factory to use
    #   other than the default BSON::ObjectId.
    # @option opts [:primary, :secondary] :read The default read preference for queries
    #   initiates from this connection object. If +:secondary+ is chosen, reads will be sent
    #   to one of the closest available secondary nodes. If a secondary node cannot be located, the
    #   read will be sent to the primary. If this option is left unspecified, the value of the read
    #   preference for this collection's associated Mongo::DB object will be used.
    #
    # @raise [InvalidNSName]
    #   if collection name is empty, contains '$', or starts or ends with '.'
    #
    # @raise [TypeError]
    #   if collection name is not a string or symbol
    #
    # @return [Collection]
    def initialize(name, db, opts={})
      if db.is_a?(String) && name.is_a?(Mongo::DB)
        warn "Warning: the order of parameters to initialize a collection have changed. " +
             "Please specify the collection name first, followed by the db. This will be made permanent" +
             "in v2.0."
        db, name = name, db
      end

      raise TypeError,
        "Collection name must be a String or Symbol." unless [String, Symbol].include?(name.class)
      name = name.to_s

      raise Mongo::InvalidNSName,
        "Collection names cannot be empty." if name.empty? || name.include?("..")

      if name.include?("$")
        raise Mongo::InvalidNSName,
          "Collection names must not contain '$'" unless name =~ /((^\$cmd)|(oplog\.\$main))/
      end

      raise Mongo::InvalidNSName,
        "Collection names must not start or end with '.'" if name.match(/^\./) || name.match(/\.$/)

      pk_factory = nil
      if opts.respond_to?(:create_pk) || !opts.is_a?(Hash)
        warn "The method for specifying a primary key factory on a Collection has changed.\n" +
             "Please specify it as an option (e.g., :pk => PkFactory)."
        pk_factory = opts
      end

      @db, @name  = db, name
      @connection = @db.connection
      @logger     = @connection.logger
      @cache_time = @db.cache_time
      @cache      = Hash.new(0)
      unless pk_factory
        @write_concern = get_write_concern(opts, db)
        @read =  opts[:read] || @db.read
        Mongo::ReadPreference::validate(@read)
        @capped             = opts[:capped]
        @tag_sets           = opts.fetch(:tag_sets, @db.tag_sets)
        @acceptable_latency = opts.fetch(:acceptable_latency, @db.acceptable_latency)
      end
      @pk_factory = pk_factory || opts[:pk] || BSON::ObjectId
      @hint = nil
      @operation_writer = CollectionOperationWriter.new(self)
      @command_writer = CollectionCommandWriter.new(self)
    end

    # Indicate whether this is a capped collection.
    #
    # @raise [Mongo::OperationFailure]
    #   if the collection doesn't exist.
    #
    # @return [Boolean]
    def capped?
      @capped ||= [1, true].include?(@db.command({:collstats => @name})['capped'])
    end

    # Return a sub-collection of this collection by name. If 'users' is a collection, then
    # 'users.comments' is a sub-collection of users.
    #
    # @param [String, Symbol] name
    #   the collection to return
    #
    # @raise [Mongo::InvalidNSName]
    #   if passed an invalid collection name
    #
    # @return [Collection]
    #   the specified sub-collection
    def [](name)
      name = "#{self.name}.#{name}"
      return Collection.new(name, db) if !db.strict? ||
        db.collection_names.include?(name.to_s)
      raise "Collection #{name} doesn't exist. Currently in strict mode."
    end

    # Set a hint field for query optimizer. Hint may be a single field
    # name, array of field names, or a hash (preferably an [OrderedHash]).
    # If using MongoDB > 1.1, you probably don't ever need to set a hint.
    #
    # @param [String, Array, OrderedHash] hint a single field, an array of
    #   fields, or a hash specifying fields
    def hint=(hint=nil)
      @hint = normalize_hint_fields(hint)
      self
    end

    # Set a hint field using a named index.
    # @param [String] hint index name
    def named_hint=(hint=nil)
      @hint = hint
      self
    end

    # Query the database.
    #
    # The +selector+ argument is a prototype document that all results must
    # match. For example:
    #
    #   collection.find({"hello" => "world"})
    #
    # only matches documents that have a key "hello" with value "world".
    # Matches can have other keys *in addition* to "hello".
    #
    # If given an optional block +find+ will yield a Cursor to that block,
    # close the cursor, and then return nil. This guarantees that partially
    # evaluated cursors will be closed. If given no block +find+ returns a
    # cursor.
    #
    # @param [Hash] selector
    #   a document specifying elements which must be present for a
    #   document to be included in the result set. Note that in rare cases,
    #   (e.g., with $near queries), the order of keys will matter. To preserve
    #   key order on a selector, use an instance of BSON::OrderedHash (only applies
    #   to Ruby 1.8).
    #
    # @option opts [Array, Hash] :fields field names that should be returned in the result
    #   set ("_id" will be included unless explicitly excluded). By limiting results to a certain subset of fields,
    #   you can cut down on network traffic and decoding time. If using a Hash, keys should be field
    #   names and values should be either 1 or 0, depending on whether you want to include or exclude
    #   the given field.
    # @option opts [:primary, :secondary] :read The default read preference for queries
    #   initiates from this connection object. If +:secondary+ is chosen, reads will be sent
    #   to one of the closest available secondary nodes. If a secondary node cannot be located, the
    #   read will be sent to the primary. If this option is left unspecified, the value of the read
    #   preference for this Collection object will be used.
    # @option opts [Integer] :skip number of documents to skip from the beginning of the result set
    # @option opts [Integer] :limit maximum number of documents to return
    # @option opts [Array]   :sort an array of [key, direction] pairs to sort by. Direction should
    #   be specified as Mongo::ASCENDING (or :ascending / :asc) or Mongo::DESCENDING (or :descending / :desc)
    # @option opts [String, Array, OrderedHash] :hint hint for query optimizer, usually not necessary if
    #   using MongoDB > 1.1
    # @option opts [String] :named_hint for specifying a named index as a hint, will be overriden by :hint
    #   if :hint is also provided.
    # @option opts [Boolean] :snapshot (false) if true, snapshot mode will be used for this query.
    #   Snapshot mode assures no duplicates are returned, or objects missed, which were preset at both the start and
    #   end of the query's execution.
    #   For details see http://www.mongodb.org/display/DOCS/How+to+do+Snapshotting+in+the+Mongo+Database
    # @option opts [Boolean] :batch_size (100) the number of documents to returned by the database per
    #   GETMORE operation. A value of 0 will let the database server decide how many results to return.
    #   This option can be ignored for most use cases.
    # @option opts [Boolean] :timeout (true) when +true+, the returned cursor will be subject to
    #   the normal cursor timeout behavior of the mongod process. When +false+, the returned cursor will
    #   never timeout. Note that disabling timeout will only work when #find is invoked with a block.
    #   This is to prevent any inadvertent failure to close the cursor, as the cursor is explicitly
    #   closed when block code finishes.
    # @option opts [Integer] :max_scan (nil) Limit the number of items to scan on both collection scans and indexed queries..
    # @option opts [Boolean] :show_disk_loc (false) Return the disk location of each query result (for debugging).
    # @option opts [Boolean] :return_key (false) Return the index key used to obtain the result (for debugging).
    # @option opts [Block] :transformer (nil) a block for transforming returned documents.
    #   This is normally used by object mappers to convert each returned document to an instance of a class.
    # @option opts [String] :comment (nil) a comment to include in profiling logs
    # @option opts [Boolean] :compile_regex (true) whether BSON regex objects should be compiled into Ruby regexes.
    #   If false, a BSON::Regex object will be returned instead.
    #
    # @raise [ArgumentError]
    #   if timeout is set to false and find is not invoked in a block
    #
    # @raise [RuntimeError]
    #   if given unknown options
    def find(selector={}, opts={})
      opts               = opts.dup
      fields             = opts.delete(:fields)
      fields             = ["_id"] if fields && fields.empty?
      skip               = opts.delete(:skip) || skip || 0
      limit              = opts.delete(:limit) || 0
      sort               = opts.delete(:sort)
      hint               = opts.delete(:hint)
      named_hint         = opts.delete(:named_hint)
      snapshot           = opts.delete(:snapshot)
      batch_size         = opts.delete(:batch_size)
      timeout            = (opts.delete(:timeout) == false) ? false : true
      max_scan           = opts.delete(:max_scan)
      return_key         = opts.delete(:return_key)
      transformer        = opts.delete(:transformer)
      show_disk_loc      = opts.delete(:show_disk_loc)
      comment            = opts.delete(:comment)
      read               = opts.delete(:read) || @read
      tag_sets           = opts.delete(:tag_sets) || @tag_sets
      acceptable_latency = opts.delete(:acceptable_latency) || @acceptable_latency
      compile_regex      = opts.key?(:compile_regex) ? opts.delete(:compile_regex) : true

      if timeout == false && !block_given?
        raise ArgumentError, "Collection#find must be invoked with a block when timeout is disabled."
      end

      if hint
        hint = normalize_hint_fields(hint)
      else
        hint = @hint        # assumed to be normalized already
      end

      raise RuntimeError, "Unknown options [#{opts.inspect}]" unless opts.empty?

      cursor = Cursor.new(self, {
        :selector           => selector,
        :fields             => fields,
        :skip               => skip,
        :limit              => limit,
        :order              => sort,
        :hint               => hint || named_hint,
        :snapshot           => snapshot,
        :timeout            => timeout,
        :batch_size         => batch_size,
        :transformer        => transformer,
        :max_scan           => max_scan,
        :show_disk_loc      => show_disk_loc,
        :return_key         => return_key,
        :read               => read,
        :tag_sets           => tag_sets,
        :comment            => comment,
        :acceptable_latency => acceptable_latency,
        :compile_regex      => compile_regex
      })

      if block_given?
        begin
          yield cursor
        ensure
          cursor.close
        end
        nil
      else
        cursor
      end
    end

    # Return a single object from the database.
    #
    # @return [OrderedHash, Nil]
    #   a single document or nil if no result is found.
    #
    # @param [Hash, ObjectId, Nil] spec_or_object_id a hash specifying elements
    #   which must be present for a document to be included in the result set or an
    #   instance of ObjectId to be used as the value for an _id query.
    #   If nil, an empty selector, {}, will be used.
    #
    # @option opts [Hash]
    #   any valid options that can be send to Collection#find
    #
    # @raise [TypeError]
    #   if the argument is of an improper type.
    def find_one(spec_or_object_id=nil, opts={})
      spec = case spec_or_object_id
             when nil
               {}
             when BSON::ObjectId
               {:_id => spec_or_object_id}
             when Hash
               spec_or_object_id
             else
               raise TypeError, "spec_or_object_id must be an instance of ObjectId or Hash, or nil"
             end
      timeout = opts.delete(:max_time_ms)
      cursor = find(spec, opts.merge(:limit => -1))
      timeout ? cursor.max_time_ms(timeout).next_document : cursor.next_document
    end

    # Save a document to this collection.
    #
    # @param [Hash] doc
    #   the document to be saved. If the document already has an '_id' key,
    #   then an update (upsert) operation will be performed, and any existing
    #   document with that _id is overwritten. Otherwise an insert operation is performed.
    #
    # @return [ObjectId] the _id of the saved document.
    #
    # @option opts [String, Integer, Symbol] :w (1) Set default number of nodes to which a write
    #   should be acknowledged.
    # @option opts [Integer] :wtimeout (nil) Set replica set acknowledgement timeout.
    # @option opts [Boolean] :j (false) If true, block until write operations have been committed
    #   to the journal. Cannot be used in combination with 'fsync'. Prior to MongoDB 2.6 this option was
    #   ignored if the server was running without journaling. Starting with MongoDB 2.6, write operations will
    #   fail with an exception if this option is used when the server is running without journaling.
    # @option opts [Boolean] :fsync (false) If true, and the server is running without journaling, blocks until
    #   the server has synced all data files to disk. If the server is running with journaling, this acts the same as
    #   the 'j' option, blocking until write operations have been committed to the journal.
    #   Cannot be used in combination with 'j'.
    #
    #   Options provided here will override any write concern options set on this collection,
    #   its database object, or the current connection. See the options
    #   for DB#get_last_error.
    #
    # @raise [Mongo::OperationFailure] will be raised iff :w > 0 and the operation fails.
    def save(doc, opts={})
      if doc.has_key?(:_id) || doc.has_key?('_id')
        id = doc[:_id] || doc['_id']
        update({:_id => id}, doc, opts.merge!({:upsert => true}))
        id
      else
        insert(doc, opts)
      end
    end

    # Insert one or more documents into the collection.
    #
    # @param [Hash, Array] doc_or_docs
    #   a document (as a hash) or array of documents to be inserted.
    #
    # @return [ObjectId, Array]
    #   The _id of the inserted document or a list of _ids of all inserted documents.
    # @return [[ObjectId, Array], [Hash, Array]]
    #   1st, the _id of the inserted document or a list of _ids of all inserted documents.
    #   2nd, a list of invalid documents.
    #   Return this result format only when :collect_on_error is true.
    #
    # @option opts [String, Integer, Symbol] :w (1) Set default number of nodes to which a write
    #   should be acknowledged.
    # @option opts [Integer] :wtimeout (nil) Set replica set acknowledgement timeout.
    # @option opts [Boolean] :j (false) If true, block until write operations have been committed
    #   to the journal. Cannot be used in combination with 'fsync'. Prior to MongoDB 2.6 this option was
    #   ignored if the server was running without journaling. Starting with MongoDB 2.6, write operations will
    #   fail with an exception if this option is used when the server is running without journaling.
    # @option opts [Boolean] :fsync (false) If true, and the server is running without journaling, blocks until
    #   the server has synced all data files to disk. If the server is running with journaling, this acts the same as
    #   the 'j' option, blocking until write operations have been committed to the journal.
    #   Cannot be used in combination with 'j'.
    #
    #   Notes on write concern:
    #     Options provided here will override any write concern options set on this collection,
    #     its database object, or the current connection. See the options for +DB#get_last_error+.
    #
    # @option opts [Boolean] :continue_on_error (+false+) If true, then
    #   continue a bulk insert even if one of the documents inserted
    #   triggers a database assertion (as in a duplicate insert, for instance).
    #   If not acknowledging writes, the list of ids returned will
    #   include the object ids of all documents attempted on insert, even
    #   if some are rejected on error. When acknowledging writes, any error will raise an
    #   OperationFailure exception.
    #   MongoDB v2.0+.
    # @option opts [Boolean] :collect_on_error (+false+) if true, then
    #   collects invalid documents as an array. Note that this option changes the result format.
    #
    # @raise [Mongo::OperationFailure] will be raised iff :w > 0 and the operation fails.
    def insert(doc_or_docs, opts={})
      if doc_or_docs.respond_to?(:collect!)
        doc_or_docs.collect! { |doc| @pk_factory.create_pk(doc) }
        error_docs, errors, write_concern_errors, rest_ignored = batch_write(:insert, doc_or_docs, true, opts)
        errors = write_concern_errors + errors
        raise errors.last if !opts[:collect_on_error] && !errors.empty?
        inserted_docs = doc_or_docs - error_docs
        inserted_ids = inserted_docs.collect {|o| o[:_id] || o['_id']}
        opts[:collect_on_error] ? [inserted_ids, error_docs] : inserted_ids
      else
        @pk_factory.create_pk(doc_or_docs)
        send_write(:insert, nil, doc_or_docs, true, opts)
        return doc_or_docs[:_id] || doc_or_docs['_id']
      end
    end
    alias_method :<<, :insert

    # Remove all documents from this collection.
    #
    # @param [Hash] selector
    #   If specified, only matching documents will be removed.
    #
    # @option opts [String, Integer, Symbol] :w (1) Set default number of nodes to which a write
    #   should be acknowledged.
    # @option opts [Integer] :wtimeout (nil) Set replica set acknowledgement timeout.
    # @option opts [Boolean] :j (false) If true, block until write operations have been committed
    #   to the journal. Cannot be used in combination with 'fsync'. Prior to MongoDB 2.6 this option was
    #   ignored if the server was running without journaling. Starting with MongoDB 2.6, write operations will
    #   fail with an exception if this option is used when the server is running without journaling.
    # @option opts [Boolean] :fsync (false) If true, and the server is running without journaling, blocks until
    #   the server has synced all data files to disk. If the server is running with journaling, this acts the same as
    #   the 'j' option, blocking until write operations have been committed to the journal.
    #   Cannot be used in combination with 'j'.
    # @option opts [Integer] :limit (0) Set limit option, currently only 0 for all or 1 for just one.
    #
    #   Notes on write concern:
    #     Options provided here will override any write concern options set on this collection,
    #     its database object, or the current connection. See the options for +DB#get_last_error+.
    #
    # @example remove all documents from the 'users' collection:
    #   users.remove
    #   users.remove({})
    #
    # @example remove only documents that have expired:
    #   users.remove({:expire => {"$lte" => Time.now}})
    #
    # @return [Hash, true] Returns a Hash containing the last error object if acknowledging writes
    #   Otherwise, returns true.
    #
    # @raise [Mongo::OperationFailure] will be raised iff :w > 0 and the operation fails.
    def remove(selector={}, opts={})
      send_write(:delete, selector, nil, nil, opts)
    end

    # Update one or more documents in this collection.
    #
    # @param [Hash] selector
    #   a hash specifying elements which must be present for a document to be updated. Note:
    #   the update command currently updates only the first document matching the
    #   given selector. If you want all matching documents to be updated, be sure
    #   to specify :multi => true.
    # @param [Hash] document
    #   a hash specifying the fields to be changed in the selected document,
    #   or (in the case of an upsert) the document to be inserted
    #
    # @option opts [Boolean] :upsert (+false+) if true, performs an upsert (update or insert)
    # @option opts [Boolean] :multi (+false+) update all documents matching the selector, as opposed to
    #   just the first matching document. Note: only works in MongoDB 1.1.3 or later.
    # @option opts [String, Integer, Symbol] :w (1) Set default number of nodes to which a write
    #   should be acknowledged.
    # @option opts [Integer] :wtimeout (nil) Set replica set acknowledgement timeout.
    # @option opts [Boolean] :j (false) If true, block until write operations have been committed
    #   to the journal. Cannot be used in combination with 'fsync'. Prior to MongoDB 2.6 this option was
    #   ignored if the server was running without journaling. Starting with MongoDB 2.6, write operations will
    #   fail with an exception if this option is used when the server is running without journaling.
    # @option opts [Boolean] :fsync (false) If true, and the server is running without journaling, blocks until
    #   the server has synced all data files to disk. If the server is running with journaling, this acts the same as
    #   the 'j' option, blocking until write operations have been committed to the journal.
    #   Cannot be used in combination with 'j'.
    #
    #   Notes on write concern:
    #     Options provided here will override any write concern options set on this collection,
    #     its database object, or the current connection. See the options for DB#get_last_error.
    #
    # @return [Hash, true] Returns a Hash containing the last error object if acknowledging writes.
    #   Otherwise, returns true.
    #
    # @raise [Mongo::OperationFailure] will be raised iff :w > 0 and the operation fails.
    def update(selector, document, opts={})
      send_write(:update, selector, document, !document.keys.first.to_s.start_with?("$"), opts)
    end

    # Create a new index.
    #
    # @param [String, Array] spec
    #   should be either a single field name or an array of
    #   [field name, type] pairs. Index types should be specified
    #   as Mongo::ASCENDING, Mongo::DESCENDING, Mongo::GEO2D, Mongo::GEO2DSPHERE, Mongo::GEOHAYSTACK,
    #   Mongo::TEXT or Mongo::HASHED.
    #
    #   Note that geospatial indexing only works with versions of MongoDB >= 1.3.3+. Keep in mind, too,
    #   that in order to geo-index a given field, that field must reference either an array or a sub-object
    #   where the first two values represent x- and y-coordinates. Examples can be seen below.
    #
    #   Also note that it is permissible to create compound indexes that include a geospatial index as
    #   long as the geospatial index comes first.
    #
    #   If your code calls create_index frequently, you can use Collection#ensure_index to cache these calls
    #   and thereby prevent excessive round trips to the database.
    #
    # @option opts [Boolean] :unique (false) if true, this index will enforce a uniqueness constraint.
    # @option opts [Boolean] :background (false) indicate that the index should be built in the background. This
    #   feature is only available in MongoDB >= 1.3.2.
    # @option opts [Boolean] :drop_dups (nil) If creating a unique index on a collection with pre-existing records,
    #   this option will keep the first document the database indexes and drop all subsequent with duplicate values.
    # @option opts [Integer] :bucket_size (nil) For use with geoHaystack indexes. Number of documents to group
    #   together within a certain proximity to a given longitude and latitude.
    # @option opts [Integer] :min (nil) specify the minimum longitude and latitude for a geo index.
    # @option opts [Integer] :max (nil) specify the maximum longitude and latitude for a geo index.
    #
    # @example Creating a compound index using a hash: (Ruby 1.9+ Syntax)
    #   @posts.create_index({'subject' => Mongo::ASCENDING, 'created_at' => Mongo::DESCENDING})
    #
    # @example Creating a compound index:
    #   @posts.create_index([['subject', Mongo::ASCENDING], ['created_at', Mongo::DESCENDING]])
    #
    # @example Creating a geospatial index using a hash: (Ruby 1.9+ Syntax)
    #   @restaurants.create_index(:location => Mongo::GEO2D)
    #
    # @example Creating a geospatial index:
    #   @restaurants.create_index([['location', Mongo::GEO2D]])
    #
    #   # Note that this will work only if 'location' represents x,y coordinates:
    #   {'location': [0, 50]}
    #   {'location': {'x' => 0, 'y' => 50}}
    #   {'location': {'latitude' => 0, 'longitude' => 50}}
    #
    # @example A geospatial index with alternate longitude and latitude:
    #   @restaurants.create_index([['location', Mongo::GEO2D]], :min => 500, :max => 500)
    #
    # @return [String] the name of the index created.
    def create_index(spec, opts={})
      opts[:dropDups]   = opts[:drop_dups] if opts[:drop_dups]
      opts[:bucketSize] = opts[:bucket_size] if opts[:bucket_size]
      field_spec        = parse_index_spec(spec)
      opts              = opts.dup
      name              = opts.delete(:name) || generate_index_name(field_spec)
      name              = name.to_s if name
      generate_indexes(field_spec, name, opts)
      name
    end

    # Calls create_index and sets a flag to not do so again for another X minutes.
    # this time can be specified as an option when initializing a Mongo::DB object as options[:cache_time]
    # Any changes to an index will be propagated through regardless of cache time (e.g., a change of index direction)
    #
    # The parameters and options for this methods are the same as those for Collection#create_index.
    #
    # @example Call sequence (Ruby 1.9+ Syntax):
    #   Time t: @posts.ensure_index(:subject => Mongo::ASCENDING) -- calls create_index and
    #     sets the 5 minute cache
    #   Time t+2min : @posts.ensure_index(:subject => Mongo::ASCENDING) -- doesn't do anything
    #   Time t+3min : @posts.ensure_index(:something_else => Mongo::ASCENDING) -- calls create_index
    #     and sets 5 minute cache
    #   Time t+10min : @posts.ensure_index(:subject => Mongo::ASCENDING) -- calls create_index and
    #     resets the 5 minute counter
    #
    # @return [String] the name of the index.
    def ensure_index(spec, opts={})
      now               = Time.now.utc.to_i
      opts[:dropDups]   = opts[:drop_dups] if opts[:drop_dups]
      opts[:bucketSize] = opts[:bucket_size] if opts[:bucket_size]
      field_spec        = parse_index_spec(spec)
      name              = opts[:name] || generate_index_name(field_spec)
      name              = name.to_s if name

      if !@cache[name] || @cache[name] <= now
        generate_indexes(field_spec, name, opts)
      end

      # Reset the cache here in case there are any errors inserting. Best to be safe.
      @cache[name] = now + @cache_time
      name
    end

    # Drop a specified index.
    #
    # @param [String] name
    def drop_index(name)
      if name.is_a?(Array)
        return drop_index(index_name(name))
      end
      @cache[name.to_s] = nil
      @db.drop_index(@name, name)
    end

    # Drop all indexes.
    def drop_indexes
      @cache = {}

      # Note: calling drop_indexes with no args will drop them all.
      @db.drop_index(@name, '*')
    end

    # Drop the entire collection. USE WITH CAUTION.
    def drop
      @db.drop_collection(@name)
    end

    # Atomically update and return a document using MongoDB's findAndModify command. (MongoDB > 1.3.0)
    #
    # @option opts [Hash] :query ({}) a query selector document for matching
    #  the desired document.
    # @option opts [Hash] :update (nil) the update operation to perform on the
    #  matched document.
    # @option opts [Array, String, OrderedHash] :sort ({}) specify a sort
    #  option for the query using any
    #  of the sort options available for Cursor#sort. Sort order is important
    #  if the query will be matching multiple documents since only the first
    #  matching document will be updated and returned.
    # @option opts [Boolean] :remove (false) If true, removes the returned
    #  document from the collection.
    # @option opts [Boolean] :new (false) If true, returns the updated
    #  document; otherwise, returns the document prior to update.
    # @option opts [Boolean] :upsert (false) If true, creates a new document
    #  if the query returns no document.
    # @option opts [Hash] :fields (nil) A subset of fields to return.
    #  Specify an inclusion of a field with 1. _id is included by default and must
    #  be explicitly excluded.
    # @option opts [Boolean] :full_response (false) If true, returns the entire
    #  response object from the server including 'ok' and 'lastErrorObject'.
    #
    # @return [Hash] the matched document.
    def find_and_modify(opts={})
      full_response = opts.delete(:full_response)

      cmd = BSON::OrderedHash.new
      cmd[:findandmodify] = @name
      cmd.merge!(opts)

      cmd[:sort] =
        Mongo::Support.format_order_clause(opts[:sort]) if opts[:sort]

      full_response ? @db.command(cmd) : @db.command(cmd)['value']
    end

    # Perform an aggregation using the aggregation framework on the current collection.
    # @note Aggregate requires server version >= 2.1.1
    # @note Field References: Within an expression, field names must be quoted and prefixed by a dollar sign ($).
    #
    # @example Define the pipeline as an array of operator hashes:
    #   coll.aggregate([ {"$project" => {"last_name" => 1, "first_name" => 1 }}, {"$match" => {"last_name" => "Jones"}} ])
    #
    # @example With server version 2.5.1 or newer, pass a cursor option to retrieve unlimited aggregation results:
    #   coll.aggregate([ {"$group" => { :_id => "$_id", :count => { "$sum" => "$members" }}} ], :cursor => {} )
    #
    # @param [Array] pipeline Should be a single array of pipeline operator hashes.
    #
    #   '$project' Reshapes a document stream by including fields, excluding fields, inserting computed fields,
    #   renaming fields,or creating/populating fields that hold sub-documents.
    #
    #   '$match' Query-like interface for filtering documents out of the aggregation pipeline.
    #
    #   '$limit' Restricts the number of documents that pass through the pipeline.
    #
    #   '$skip' Skips over the specified number of documents and passes the rest along the pipeline.
    #
    #   '$unwind' Peels off elements of an array individually, returning one document for each member.
    #
    #   '$group' Groups documents for calculating aggregate values.
    #
    #   '$sort' Sorts all input documents and returns them to the pipeline in sorted order.
    #
    #   '$out' The name of a collection to which the result set will be saved.
    #
    # @option opts [:primary, :secondary] :read Read preference indicating which server to perform this operation
    #  on. If $out is specified and :read is not :primary, the aggregation will be rerouted to the primary with
    #  a warning. See Collection#find for more details.
    # @option opts [String]  :comment (nil) a comment to include in profiling logs
    # @option opts [Hash] :cursor return a cursor object instead of an Array.  Takes an optional batchSize parameter
    #  to specify the maximum size, in documents, of the first batch returned.
    #
    # @return [Array] An Array with the aggregate command's results.
    #
    # @raise MongoArgumentError if operators either aren't supplied or aren't in the correct format.
    # @raise MongoOperationFailure if the aggregate command fails.
    #
    def aggregate(pipeline=nil, opts={})
      raise MongoArgumentError, "pipeline must be an array of operators" unless pipeline.class == Array
      raise MongoArgumentError, "pipeline operators must be hashes" unless pipeline.all? { |op| op.class == Hash }

      selector = BSON::OrderedHash.new
      selector['aggregate'] = self.name
      selector['pipeline'] = pipeline

      result = @db.command(selector, command_options(opts))
      unless Mongo::Support.ok?(result)
        raise Mongo::OperationFailure, "aggregate failed: #{result['errmsg']}"
      end

      if result.key?('cursor')
        cursor_info = result['cursor']

        seed = {
          :cursor_id => cursor_info['id'],
          :first_batch => cursor_info['firstBatch'],
          :pool => @connection.pinned_pool
        }

        return Cursor.new(self, seed.merge!(opts))

      elsif selector['pipeline'].any? { |op| op.key?('$out') || op.key?(:$out) }
        return result
      end

      result['result'] || result
    end

    # Perform a map-reduce operation on the current collection.
    #
    # @param [String, BSON::Code] map a map function, written in JavaScript.
    # @param [String, BSON::Code] reduce a reduce function, written in JavaScript.
    #
    # @option opts [Hash] :query ({}) a query selector document, like what's passed to #find, to limit
    #   the operation to a subset of the collection.
    # @option opts [Array] :sort ([]) an array of [key, direction] pairs to sort by. Direction should
    #   be specified as Mongo::ASCENDING (or :ascending / :asc) or Mongo::DESCENDING (or :descending / :desc)
    # @option opts [Integer] :limit (nil) if passing a query, number of objects to return from the collection.
    # @option opts [String, BSON::Code] :finalize (nil) a javascript function to apply to the result set after the
    #   map/reduce operation has finished.
    # @option opts [String, Hash] :out Location of the result of the map-reduce operation. You can output to a
    #   collection, output to a collection with an action, or output inline. You may output to a collection
    #   when performing map reduce operations on the primary members of the set; on secondary members you
    #   may only use the inline output. See the server mapReduce documentation for available options.
    # @option opts [Boolean] :keeptemp (false) if true, the generated collection will be persisted. The default
    #   is false. Note that this option has no effect is versions of MongoDB > v1.7.6.
    # @option opts [Boolean ] :verbose (false) if true, provides statistics on job execution time.
    # @option opts [Boolean] :raw (false) if true, return the raw result object from the map_reduce command, and not
    #   the instantiated collection that's returned by default. Note if a collection name isn't returned in the
    #   map-reduce output (as, for example, when using :out => { :inline => 1 }), then you must specify this option
    #   or an ArgumentError will be raised.
    # @option opts [:primary, :secondary] :read Read preference indicating which server to run this map-reduce
    #  on. See Collection#find for more details.
    # @option opts [String]  :comment (nil) a comment to include in profiling logs
    #
    # @return [Collection, Hash] a Mongo::Collection object or a Hash with the map-reduce command's results.
    #
    # @raise ArgumentError if you specify { :out => { :inline => true }} but don't specify :raw => true.
    #
    # @see http://www.mongodb.org/display/DOCS/MapReduce Offical MongoDB map/reduce documentation.
    def map_reduce(map, reduce, opts={})
      opts = opts.dup
      map    = BSON::Code.new(map) unless map.is_a?(BSON::Code)
      reduce = BSON::Code.new(reduce) unless reduce.is_a?(BSON::Code)
      raw    = opts.delete(:raw)

      hash = BSON::OrderedHash.new
      hash['mapreduce'] = self.name
      hash['map'] = map
      hash['reduce'] = reduce
      hash['out'] = opts.delete(:out)
      hash['sort'] = Mongo::Support.format_order_clause(opts.delete(:sort)) if opts.key?(:sort)

      result = @db.command(hash, command_options(opts))
      unless Mongo::Support.ok?(result)
        raise Mongo::OperationFailure, "map-reduce failed: #{result['errmsg']}"
      end

      if raw
        result
      elsif result['result']
        if result['result'].is_a?(BSON::OrderedHash) &&
            result['result'].key?('db') &&
            result['result'].key?('collection')
          otherdb = @db.connection[result['result']['db']]
          otherdb[result['result']['collection']]
        else
          @db[result["result"]]
        end
      else
        raise ArgumentError, "Could not instantiate collection from result. If you specified " +
          "{:out => {:inline => true}}, then you must also specify :raw => true to get the results."
      end
    end
    alias :mapreduce :map_reduce

    # Perform a group aggregation.
    #
    # @param [Hash] opts the options for this group operation. The minimum required are :initial
    #   and :reduce.
    #
    # @option opts [Array, String, Symbol] :key (nil) Either the name of a field or a list of fields to group by (optional).
    # @option opts [String, BSON::Code] :keyf (nil) A JavaScript function to be used to generate the grouping keys (optional).
    # @option opts [String, BSON::Code] :cond ({}) A document specifying a query for filtering the documents over
    #   which the aggregation is run (optional).
    # @option opts [Hash] :initial the initial value of the aggregation counter object (required).
    # @option opts [String, BSON::Code] :reduce (nil) a JavaScript aggregation function (required).
    # @option opts [String, BSON::Code] :finalize (nil) a JavaScript function that receives and modifies
    #   each of the resultant grouped objects. Available only when group is run with command
    #   set to true.
    # @option opts [:primary, :secondary] :read Read preference indicating which server to perform this group
    #  on. See Collection#find for more details.
    # @option opts [String]  :comment (nil) a comment to include in profiling logs
    #
    # @return [Array] the command response consisting of grouped items.
    def group(opts, condition={}, initial={}, reduce=nil, finalize=nil)
      opts = opts.dup
      if opts.is_a?(Hash)
        return new_group(opts)
      elsif opts.is_a?(Symbol)
        raise MongoArgumentError, "Group takes either an array of fields to group by or a JavaScript function" +
          "in the form of a String or BSON::Code."
      end

      warn "Collection#group no longer takes a list of parameters. This usage is deprecated and will be removed in v2.0." +
             "Check out the new API at http://api.mongodb.org/ruby/current/Mongo/Collection.html#group-instance_method"

      reduce = BSON::Code.new(reduce) unless reduce.is_a?(BSON::Code)

      group_command = {
        "group" => {
          "ns"      => @name,
          "$reduce" => reduce,
          "cond"    => condition,
          "initial" => initial
        }
      }

      unless opts.nil?
        if opts.is_a? Array
          key_type = "key"
          key_value = {}
          opts.each { |k| key_value[k] = 1 }
        else
          key_type  = "$keyf"
          key_value = opts.is_a?(BSON::Code) ? opts : BSON::Code.new(opts)
        end

        group_command["group"][key_type] = key_value
      end

      finalize = BSON::Code.new(finalize) if finalize.is_a?(String)
      if finalize.is_a?(BSON::Code)
        group_command['group']['finalize'] = finalize
      end

      result = @db.command(group_command)

      if Mongo::Support.ok?(result)
        result["retval"]
      else
        raise OperationFailure, "group command failed: #{result['errmsg']}"
      end
    end

    # Scan this entire collection in parallel.
    # Returns a list of up to num_cursors cursors that can be iterated concurrently. As long as the collection
    # is not modified during scanning, each document appears once in one of the cursors' result sets.
    #
    # @note Requires server version >= 2.5.5
    #
    # @param [Integer] num_cursors the number of cursors to return.
    # @param [Hash] opts
    #
    # @return [Array] An array of up to num_cursors cursors for iterating over the collection.
    def parallel_scan(num_cursors, opts={})
      cmd                          = BSON::OrderedHash.new
      cmd[:parallelCollectionScan] = self.name
      cmd[:numCursors]             = num_cursors
      result                       = @db.command(cmd, command_options(opts))

      result['cursors'].collect do |cursor_info|
        seed = {
          :cursor_id   => cursor_info['cursor']['id'],
          :first_batch => cursor_info['cursor']['firstBatch'],
          :pool        => @connection.pinned_pool
        }
        Cursor.new(self, seed.merge!(opts))
      end

    end

    private

    def new_group(opts={})
      reduce   =  opts.delete(:reduce)
      finalize =  opts.delete(:finalize)
      cond     =  opts.delete(:cond) || {}
      initial  =  opts.delete(:initial)

      if !(reduce && initial)
        raise MongoArgumentError, "Group requires at minimum values for initial and reduce."
      end

      cmd = {
        "group" => {
          "ns"      => @name,
          "$reduce" => reduce.to_bson_code,
          "cond"    => cond,
          "initial" => initial
        }
      }

      if finalize
        cmd['group']['finalize'] = finalize.to_bson_code
      end

      if key = opts.delete(:key)
        if key.is_a?(String) || key.is_a?(Symbol)
          key = [key]
        end
        key_value = {}
        key.each { |k| key_value[k] = 1 }
        cmd["group"]["key"] = key_value
      elsif keyf = opts.delete(:keyf)
        cmd["group"]["$keyf"] = keyf.to_bson_code
      end

      result = @db.command(cmd, command_options(opts))
      result["retval"]
    end

    public

    # Return a list of distinct values for +key+ across all
    # documents in the collection. The key may use dot notation
    # to reach into an embedded object.
    #
    # @param [String, Symbol, OrderedHash] key or hash to group by.
    # @param [Hash] query a selector for limiting the result set over which to group.
    # @param [Hash] opts the options for this distinct operation.
    #
    # @option opts [:primary, :secondary] :read Read preference indicating which server to perform this query
    #  on. See Collection#find for more details.
    # @option opts [String]  :comment (nil) a comment to include in profiling logs
    #
    # @example Saving zip codes and ages and returning distinct results.
    #   @collection.save({:zip => 10010, :name => {:age => 27}})
    #   @collection.save({:zip => 94108, :name => {:age => 24}})
    #   @collection.save({:zip => 10010, :name => {:age => 27}})
    #   @collection.save({:zip => 99701, :name => {:age => 24}})
    #   @collection.save({:zip => 94108, :name => {:age => 27}})
    #
    #   @collection.distinct(:zip)
    #     [10010, 94108, 99701]
    #   @collection.distinct("name.age")
    #     [27, 24]
    #
    #   # You may also pass a document selector as the second parameter
    #   # to limit the documents over which distinct is run:
    #   @collection.distinct("name.age", {"name.age" => {"$gt" => 24}})
    #     [27]
    #
    # @return [Array] an array of distinct values.
    def distinct(key, query=nil, opts={})
      raise MongoArgumentError unless [String, Symbol].include?(key.class)
      command            = BSON::OrderedHash.new
      command[:distinct] = @name
      command[:key]      = key.to_s
      command[:query]    = query

      @db.command(command, command_options(opts))["values"]
    end

    # Rename this collection.
    #
    # Note: If operating in auth mode, the client must be authorized as an admin to
    # perform this operation.
    #
    # @param [String] new_name the new name for this collection
    #
    # @return [String] the name of the new collection.
    #
    # @raise [Mongo::InvalidNSName] if +new_name+ is an invalid collection name.
    def rename(new_name)
      case new_name
      when Symbol, String
      else
        raise TypeError, "new_name must be a string or symbol"
      end

      new_name = new_name.to_s

      if new_name.empty? or new_name.include? ".."
        raise Mongo::InvalidNSName, "collection names cannot be empty"
      end
      if new_name.include? "$"
        raise Mongo::InvalidNSName, "collection names must not contain '$'"
      end
      if new_name.match(/^\./) or new_name.match(/\.$/)
        raise Mongo::InvalidNSName, "collection names must not start or end with '.'"
      end

      @db.rename_collection(@name, new_name)
      @name = new_name
    end

    # Get information on the indexes for this collection.
    #
    # @return [Hash] a hash where the keys are index names.
    def index_information
      @db.index_information(@name)
    end

    # Return a hash containing options that apply to this collection.
    # For all possible keys and values, see DB#create_collection.
    #
    # @return [Hash] options that apply to this collection.
    def options
      @db.collections_info(@name).next_document['options']
    end

    # Return stats on the collection. Uses MongoDB's collstats command.
    #
    # @return [Hash]
    def stats
      @db.command({:collstats => @name})
    end

    # Get the number of documents in this collection.
    #
    # @option opts [Hash] :query ({}) A query selector for filtering the documents counted.
    # @option opts [Integer] :skip (nil) The number of documents to skip.
    # @option opts [Integer] :limit (nil) The number of documents to limit.
    # @option opts [:primary, :secondary] :read Read preference for this command. See Collection#find for
    #  more details.
    # @option opts [String]  :comment (nil) a comment to include in profiling logs
    #
    # @return [Integer]
    def count(opts={})
      find(opts[:query],
           :skip  => opts[:skip],
           :limit => opts[:limit],
           :read  => opts[:read],
           :comment => opts[:comment]).count(true)
    end

    alias :size :count

    protected

    # Provide required command options if they are missing in the command options hash.
    #
    # @return [Hash] The command options hash
    def command_options(opts)
      opts[:read] ? opts : opts.merge(:read => @read)
    end

    def normalize_hint_fields(hint)
      case hint
      when String
        {hint => 1}
      when Hash
        hint
      when nil
        nil
      else
        h = BSON::OrderedHash.new
        hint.to_a.each { |k| h[k] = 1 }
        h
      end
    end

    private

    def send_write(op_type, selector, doc_or_docs, check_keys, opts, collection_name=@name)
      write_concern = get_write_concern(opts, self)
      if @db.connection.use_write_command?(write_concern)
        @command_writer.send_write_command(op_type, selector, doc_or_docs, check_keys, opts, write_concern, collection_name)
      else
        @operation_writer.send_write_operation(op_type, selector, doc_or_docs, check_keys, opts, write_concern, collection_name)
      end
    end

    def index_name(spec)
      field_spec = parse_index_spec(spec)
      index_information.each do |index|
        return index[0] if index[1]['key'] == field_spec
      end
      nil
    end

    def parse_index_spec(spec)
      field_spec = BSON::OrderedHash.new
      if spec.is_a?(String) || spec.is_a?(Symbol)
        field_spec[spec.to_s] = 1
      elsif spec.is_a?(Hash)
        if RUBY_VERSION < '1.9' && !spec.is_a?(BSON::OrderedHash)
          raise MongoArgumentError, "Must use OrderedHash in Ruby < 1.9.0"
        end
        validate_index_types(spec.values)
        field_spec = spec.is_a?(BSON::OrderedHash) ? spec : BSON::OrderedHash.try_convert(spec)
      elsif spec.is_a?(Array) && spec.all? {|field| field.is_a?(Array) }
        spec.each do |f|
          validate_index_types(f[1])
          field_spec[f[0].to_s] = f[1]
        end
      else
        raise MongoArgumentError, "Invalid index specification #{spec.inspect}; " +
          "should be either a hash (OrderedHash), string, symbol, or an array of arrays."
      end
      field_spec
    end

    def validate_index_types(*types)
      types.flatten!
      types.each do |t|
        unless Mongo::INDEX_TYPES.values.include?(t)
          raise MongoArgumentError, "Invalid index field #{t.inspect}; " +
                "should be one of " + Mongo::INDEX_TYPES.map {|k,v| "Mongo::#{k} (#{v})"}.join(', ')
        end
      end
    end

    def generate_indexes(field_spec, name, opts)
      selector = {
        :name   => name,
        :key    => field_spec
      }
      selector.merge!(opts)

      begin
        cmd = BSON::OrderedHash[:createIndexes, @name, :indexes, [selector]]
        @db.command(cmd)
      rescue Mongo::OperationFailure => ex
        if Mongo::ErrorCode::COMMAND_NOT_FOUND_CODES.include?(ex.error_code)
          selector[:ns] = "#{@db.name}.#{@name}"
          send_write(:insert, nil, selector, false, {:w => 1}, Mongo::DB::SYSTEM_INDEX_COLLECTION)
        else
          raise Mongo::OperationFailure, "Failed to create index #{selector.inspect} with the following error: " +
           "#{ex.message}"
        end
      end

      nil
    end

    def generate_index_name(spec)
      indexes = []
      spec.each_pair do |field, type|
        indexes.push("#{field}_#{type}")
      end
      indexes.join("_")
    end

    def batch_write(op_type, documents, check_keys=true, opts={})
      write_concern = get_write_concern(opts, self)
      if @db.connection.use_write_command?(write_concern)
        return @command_writer.batch_write(op_type, documents, check_keys, opts)
      else
        return @operation_writer.batch_write(op_type, documents, check_keys, opts)
      end
    end

  end

end

# Copyright (C) 2009-2014 MongoDB, Inc.
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

  # Represents a collection in the database and operations that can directly be
  # applied to one.
  #
  # @since 2.0.0
  class Collection
    extend Forwardable
    include Indexable

    # @return [ Mongo::Database ] The database the collection resides in.
    attr_reader :database

    # @return [ String ] The name of the collection.
    attr_reader :name

    # Get client, cluser and server preference from client.
    def_delegators :@database, :client, :cluster

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
      name == other.name && database == other.database
    end

    # Get the server (read) preference from the options passed to this collection.
    #
    # @param [ Hash ] opts Options from a query
    #
    # @todo fix this to work with db server_preference
    #
    # @since 2.0.0
    def server_preference(opts={})
      return ServerPreference.get(opts[:read]) if opts[:read]
      @server_preference || database.server_preference
    end

    # Instantiate a new collection object.
    #
    # @param [ Mongo::Database ] database The database this collection belongs to.
    # @param [ String ] name The name of the collection.
    # @param [ Hash ] opts Options for this collection.
    #
    # @option opts [:create_pk] :pk (BSON::ObjectId) A primary key factory to use other
    #  than the default BSON::ObjectId
    # @option opts [ true, false ] :capped (false) Create a capped collection.
    # @option opts [ Integer] :size (Nil) If :capped is true, specified the max number
    #  of bytes for the capped collection.  Otherwise, specifies the number of bytes
    #  allocated for the initial extent of the collection.
    # @option opts [ Integer ] :max (Nil) If :capped is true, indicates the maximum
    #  number of records in the capped collection.
    # @option opts [ Symbol ] :read A read preference for the collection.
    #
    # @since 2.0.0
    def initialize(database, name, opts={})
      Collection.validate_name(name)
      @database = database
      @name = name.to_s

      @capped            = opts[:capped]
      @server_preference = ServerPreference.get(opts[:read]) if opts[:read]
      @pk_factory        = opts[:pk] if opts[:pk]
    end

    # Perform an aggregation using the aggregation framework on the current collection.
    #
    # @param [ Array ] pipeline A single array of pipeline operator hashes.
    # @param [ Hash ] opts Options for this aggregation query.
    #   '$project' Reshapes a document stream by including fields, excluding fields,
    #   inserting computed fields, renaming fields, or creating/populating fields that
    #   hold sub-documents.
    #
    #   '$match' Query-like interface for filtering documents out of the aggregation
    #   pipeline.
    #
    #   '$limit' Restricts the number of documents that pass through the pipeline.
    #
    #   '$skip' Skips over the specified number of documents and passes the rest along
    #   the pipeline.
    #
    #   '$unwind' Peels off elements of an array individually, returning one document
    #   for each member.
    #
    #   '$group' Groups documents for calculating aggregate values.
    #
    #   '$sort' Sorts input documents and returns them to the pipeline in sorted order.
    #
    #   '$out' The name of a collection to which the result set will be saved.
    #
    # @option opts [ Symbol ] :read The read preference for this operation.
    # @option opts [ String ] :comment A comment to include in profiling logs.
    # @option opts [ Hash ] :cursor Return a cursor object instead of an Array.  Takes
    #  an optional batchSize parameter to specify the maximum size, in documents, of
    #  the first batch returned.
    #
    # @return [ Array, Cursor ] the results of this aggregation query.
    #
    # @since 2.0.0
    def aggregate(pipeline = nil, opts = {})
      unless pipeline.is_a?(Array)
        raise MongoArgumentError, "pipeline must be an array of operators"
      end
      unless pipeline.all? { |op| op.is_a?(Hash) }
        raise MongoArgumentError, "pipeline operators must be hashes"
      end

      result = db.command({ :aggregate => name,
                            :pipeline  => pipeline },
                          opts)
      # @todo - check for operation failure here and raise error
      # @todo - finish coding
    end

    # Is this a capped collection?
    #
    # @return [ true, false ] whether this collection is capped.
    #
    # @since 2.0.0
    def capped?
      @capped ||= [1, true].include?(database.command({:collstats => name})['capped'])
    end

    # Get the number of documents in this collection.
    #
    # @param [ Hash ] opts Options for this operation.
    #
    # @option opts [ Hash ] :query ({}) A selector for filtering the documents in this
    #  collection.
    # @option opts [ Integer ] :skip (nil) The number of documents to skip.
    # @option opts [ Integer ] :limit (nil) The number of documents to limit.
    # @option opts [ Symbol ] :read The read preference for this operation.
    # @option opts [ Hash, String ] :hint (nil) The index name or spec to use.
    # @option opts [ String ] :comment (nil) A comment to include in profiling logs.
    #
    # @return [ Integer ] the number of documents.
    #
    # @since 2.0.0
    def count(opts={})
      cmd = { :count => name, :query => opts[:query] || {} }
      cmd[:skip]  = opts[:skip]  if opts[:skip]
      cmd[:limit] = opts[:limit] if opts[:limit]
      cmd[:hint]  = opts[:hint]  if opts[:hint]
      r = database.command(cmd)
      puts r
      r["n"]
    end
    alias :size :count

    # Return a list of distinct values for 'key' across all documents in the
    #  collection. The key may not use dot notation to reach into an embedded object.
    #
    # @param [ String, Symbol, Hash ] a key or hash to group by.
    # @param [ Hash ] query (nil) A selector for limiting the result set over which to
    #  count distinct values.
    # @param [ Hash ] opts Options for this operation.
    #
    # @option opts [ Symbol ] :read Read preference for this query.
    # @option opts [ String ] :comment (nil) A comment to include for profiling.
    #
    # @return [ Array ] an array of distinct values.
    #
    # @since 2.0.0
    def distinct(key, query=nil, opts={})
      database.command({ :distinct => name,
                         :key      => key.to_s,
                         :query    => query },
                       opts )["values"]
    end

    # Drops the entire collection.  USE WITH CAUTION, THIS CANNOT BE UNDONE.
    #
    # @since 2.0.0
    def drop
      database.drop_collection(name)
    end

    # Query the database
    #
    # @example collection.find({ :name => "sam" })
    # @example collection.find({ :age => { "$gt" => 30 }},
    #                          { :sort => [[ :name, Mongo::ASCENDING ]]},
    #                          { :skip => 10 })
    #
    # @param [ Hash ] selector A Hash specifying elements which must be present
    #  for a document to be included in the result set.
    # @param [ Hash ] opts Options for this query.
    #
    # @option opts [ Array, Hash] :fields field names that should be returned
    #  in the result set.  If a Hash, keys should be field names and values should
    #  be either 0 (to exclude) or 1 (to include).
    # @option opts [ :primary, :secondary ] :read The default read preference for
    #  this query or set of queries.  If +:secondary+ is chosen and a secondary
    #  cannot be reached, the read will be routed to the primary instead. If not
    #  specified, the default read preference for this collection will be used.
    # @option opts [ Integer ] :skip The number of documents to skip in the result set.
    # @option opts [ Integer ] :limit The number of documents to limit this result set
    #  to.
    # @option opts [ Array ] :sort An array of [key, direction] pairs to sort by.
    #  Direction should be specified as Mongo::ASCENDING or Mongo::DESCENDING.
    # @option opts [ String, Array, Hash ] :hint Hint for the query optimizer.
    # @option opts [ String ] :named_hint for specifying a named index as a hint, will
    #  be overriden by :hint if :hint is provided.
    # @option opts [ true, false ] :snapshot (false) If true, snapshot mode will be
    #  used for this query.  Snapshot mode assures that no duplicates are returned, or
    #  objects missed, which were present at both the start and end of the query's
    #  execution.  For more information see:
    #  http://www.mongodb.org/display/DOCS/How+to+do+Snapshotting+in+the+Mongo+Database
    # @option opts [ Integer ] :batch_size (100) The number of documents to retun from
    #  the database per GETMORE operation.  A value of 0 will let the database decide
    #  how many results to return.
    # @option opts [ true, false ] :timeout (true) When +true+, the returned cursor
    #  will be subject to the normal cursor timeout behavior for the mongod process.
    #  When +false+, the returned cursor will never timeout.  Note that disabling
    #  timeout is only allowed when #find is invoked with a block.
    # @option opts [ Integer ] :max_scan (nil) Limit the number of items to scan on
    #  both collection scans and indexed queries.
    # @option opts [ true, false ] :show_disk_loc (false) Return the fisk location of
    #  each result (for debugging)
    # @option opts [ true, false ] :return_key (false) Return the indexed key used to
    #  obtain the result (for debugging)
    # @option opts [ Block ] :transformer (nil) A block for transforming returned
    #  documents.  This is normally used by object mappers to convert documents to an
    #  instance of a class.
    # @option opts [ String ] :comment (nil) A comment to include in profiling logs.
    # @option opts [ true, false ] :compile_regex (true) Whether BSON regex objects
    #  should be compiled into Ruby regexes.  If false, a BSON::Regex object will be
    #  returned instead.
    #
    # @todo - investigate these options: max_time_ms, max/min, compile_regex (CV),
    #  transformer, return_key (CV), named_hint, tag_sets, acceptable_latency
    #
    # @return [ CollectionView ]
    #
    # @since 2.0.0
    def find(selector={}, opts={})

      if opts[:timeout] == false && !block_given?
        raise Mongo::ArgumentError,
        "Collection#find must be used with a block when timeout is disabled"
      end

      # @todo - handle bad options here?
      opts = opts.dup
      if named_hint = opts.delete(:named_hint)
        opts[:hint] ||= named_hint
      end

      cv = CollectionView.new(self, selector, opts)
      if block_given?
        cv.each do |doc|
          yield doc
        end
        # @todo - how to close this?
      else
        cv
      end
    end

    # Return a single document from the database.
    #
    # @param [ Hash ] selector A query selector to filter documents.
    # @param [ Hash ] opts Options for this query.
    #
    # @todo - will there be options for this?
    #
    # @return [ Hash ] the matched document.
    def find_one(selector={}, opts={})
      opts[:limit] = 1
      find(selector, opts).first
    end

    # Insert one or more documents into the collection.
    #
    # @param [ Hash, Array ] doc_or_docs The document or documents to insert.
    # @param [ Hash ] opts Options.
    #
    # @option opts [ String, Integer, Symbol ] :w (1) Set the write concern.
    # @option opts [ Integer ] :wtimeout (nil) Set replica set acknowledgment timeout.
    # @option opts [ true, false ] :j (false) If true, block until write operations
    #  have been committed to the journal. Cannot be used with 'fsync'.  Prior to
    #  MongoDB 2.6 this option was ignored if the server was running with journaling.
    #  Starting with MongoDB 2.6, write operations fail with an exception if this
    #  option is used when the server is running without journaling.
    # @option opts [ true, false ] :fsync (false) If true and the server is running
    #  without journaling, blocks until the server has synced all data files to disk.
    #  If the server is running without journaling, this acts like the 'j' option,
    #  blocking until write operations have been committed to the journal.  Cannot be
    #  used in combination with 'j'.
    # @option opts [ true, false ] :continue_on_error (false) If true, then continue a
    #  bulk insert even if one of the documents inserted triggers a database assertion
    #  (as in a duplicate insert, for instance).  If not acknowledging writes, the list
    #  of ids returned will include values for all documents it attempted to insert,
    #  even if some of those were rejected on error.  When acknowledging writes, any
    #  error will raise an OperationFailure exception.  MongoDB 2.0+.
    #
    # @return [ BSON::ObjectId, Array ] the _id or _ids of the inserted document(s).
    #
    # @since 2.0.0
    def insert(docs, opts={})
      validate_opts(opts)

      docs = docs.is_a?(Array) ? docs : [ docs ]
      docs.collect! { |doc| add_pk!(doc) }

      op = Operation::Write::Insert.new({ :documents => docs,
                                          :db_name   => database.name,
                                          :coll_name => name,
                                          :write_concern => write_concern(opts),
                                          :opts => opts.merge(:limit => -1) })

      res = op.execute(get_context({}, true))
      docs.collect! { |doc| doc[:_id] } if res.documents[0]["ok"]
      res = docs.length == 1 ? docs[0] : docs
      # @todo - the CRUD api suggests an InsertResponse type that would
      # include a field 'document' and a field 'insertedId'.  Not final.
    end
    alias :<< :insert

    # Perform a map-reduce operation on the current collection.
    #
    # @param [ String, BSON::Code ] map A map function, written in JavaScript.
    # @param [ String, BSON::Code ] reduce A reduce function, written in JavaScript.
    # @param [ Hash ] opts Options for this operation.
    #
    # @todo - finish commenting this.
    #
    # @since 2.0.0
    def map_reduce(map, reduce, opts={})
      # @todo
    end
    alias :mapreduce :map_reduce

    # Scan this entire collection in parallel.  Returns a list of up to num_cursors
    #  cursors that can be iterated concurrently.  As long as the collection is not
    #  modified during scanning, each document appears once in one of the cursors'
    #  result sets.
    #
    # @note - this requires MongoDB version >= 2.5.5
    #
    # @param [ Integer ] num_cursors The max number of cursors to return.
    # @param [ Hash ] opts Options for this operation.
    #
    # @todo - what are options?
    #
    # @since 2.0.0
    def parallel_scan(num_cursors, opts={})
      result = db.command({ :parallelCollectionScan => name,
                            :numCursors             => num_cursors })
      # @todo - make this into cursors
      # @todo - finish...
    end

    # Removes all matching documents from the collection.
    #
    # @note - {} as a selector here will have no effect, use remove_all
    #  to remove all documents from the collection.
    #
    # @param [ Hash ] selector Only matching documents will be removed.
    # @param [ Hash ] opts Options for this query
    #
    # @option opts [ String, Integer, Symbol ] :w (1) Set default number of nodes to
    #  which a write must be acknowledged.
    # @option opts [ Integer ] :wtimeout (nil) Set replica set acknowledgement timeout.
    # @option opts [ true, false ] :j (false) If true, block until write operations
    #  have been committed to the journal.  Cannot be used in combination with 'fsync.'
    #  Prior to MongoDB 2.6 this option was ignored if the server was running without
    #  journaling.  Starting with MongoDB 2.6, write operations will raise an exception
    #  if this opton is used when the server is running without journaling.
    # @option opts [ true, false ] :fsync (false) If true, and the server is running
    #  without jornaling, blocks until the server has synced all data files to disk.
    #  If the server is running with journaling, this acts the same as the 'j' option,
    #  blocking until write operations have been committed to the journal.  Cannot be
    #  used in combination with the 'j' option.
    # @option opts [ Integer ] :limit (0) Set limit option, currently only 0 for all or
    #  1 for just one.
    #
    # @example remove all expired documents:
    #  users.remove({ :expire => { "lte" => Time.now }})
    #
    # @return [ Hash, true ] Returns a Hash containing the last error object if
    #  acknowledging writes, otherwise return true.
    #
    # @since 2.0.0
    def remove(selector={}, opts={})
      # @todo - should this error if selector is empty?
      return if selector.empty?

      validate_opts(opts)
      if opts[:limit] && (opts[:limit] != 0 && opts[:limit] != 1)
        raise Mongo::ArgumentError, "The limit for a remove operation must be 0 or 1"
      end

      query = [ { :q => selector, :limit => opts[:limit] || 0 } ]
      op = Operation::Write::Delete.new({ :deletes       => query,
                                          :db_name       => database.name,
                                          :coll_name     => name,
                                          :write_concern => write_concern(opts) })
      response = op.execute(get_context(opts, true))
      # @todo - revisit once remove response / server preference is done.
    end

    # Removes all documents from the collection.  USE WITH CAUTION.
    #
    # @param [ Hash ] opts Options for this query
    #
    # @option opts [ String, Integer, Symbol ] :w (1) Set default number of nodes to
    #  which a write must be acknowledged.
    # @option opts [ Integer ] :wtimeout (nil) Set replica set acknowledgement timeout.
    # @option opts [ true, false ] :j (false) If true, block until write operations
    #  have been committed to the journal.  Cannot be used in combination with 'fsync.'
    #  Prior to MongoDB 2.6 this option was ignored if the server was running without
    #  journaling.  Starting with MongoDB 2.6, write operations will raise an exception
    #  if this opton is used when the server is running without journaling.
    # @optoin opts [ true, false ] :fsync (false) If true, and the server is running
    #  without jornaling, blocks until the server has synced all data files to disk.
    #  If the server is running with journaling, this acts the same as the 'j' option,
    #  blocking until write operations have been committed to the journal.  Cannot be
    #  used in combination with the 'j' option.
    #
    # @return [ Hash, true ] Returns a Hash containing the last error object if
    #  acknowledging writes, otherwise return true.
    #
    # @since 2.0.0
    def remove_all(opts={})
      validate_opts(opts)
      op = Write::Delete.new({ :deletes       => [{}],
                               :db_name       => database.name,
                               :coll_name     => name,
                               :write_concern => write_concern(opts) })
      response = op.execute(@read.server.context)
      # @todo - continue parsing
      # @todo - revisit once remove response / server preference is done.
    end

    # Rename this collection.
    #
    # @note If operating in auth mode, the client must be authorized as an admin to
    #  perform this operation.
    #
    # @param [ String ] new_name The new collection name.
    # @param [ true, false ] drop (true) drop If true and there is already a collection
    #  in this database with the name 'new_name', drop the target collection before
    #  replacing it with this collection.  If false and such a collection exists, this
    #  operation will raise an error.
    #
    # @since 2.0.0
    def rename(new_name, drop=true)
      database.rename_collection(name, new_name, drop)
      @name = new_name
    end

    # Save a document to this collection.  If the document already has an '_id' key,
    #  then an update (upsert) operation will be performed, and any existing document
    #  with that _id will be overwritten.  Otherwise, an insert is performed.
    #
    # @param [ Hash ] doc The document to be saved.
    # @param [ Hash ] opts Options for this operation.
    #
    # @todo - option, read pref
    #
    # @since 2.0.0
    def save(doc, opts={})
      if doc.has_key?(:_id) || doc.has_key?('id')
        id = doc[:_id] || doc['_id']
        find({ :_id => id }, opts).upsert.replace_one(doc)
      else
        insert(doc, opts)
      end
    end

    # Run MongoDB's collstats command to return statistics on this collection.
    #
    # @return [ Hash ] statistics on this collection.
    #
    # @since 2.0.0
    def stats
      database.command({ :collstats => name, :scale => 1024 })
    end

    # Raise an error if this string is not a valid collection name.
    #
    # @param [ String, Symbol ] s The proposed collection name.
    #
    # @since 2.0.0
    def self.validate_name(s)
      s = s.to_s
      raise InvalidName.new unless s
      raise InvalidName.new("Collection name cannot be empty") if s.empty?
      raise InvalidName.new("Collection name cannot contain '..'") if s.include?('..')
      raise InvalidName.new("Collection name cannot contain null") if s.include?('\0')
      if s.include?('$')
        raise InvalidName.new("Collection name must not contain '$'")
      end
      if s.match(/^\./) or s.match(/\.$/)
        raise InvalidName.new("Collection name must not start or end with '.'")
      end
    end

    # Exception that is raised when trying to create a collection with no name.
    #
    # @since 2.0.0
    class InvalidName < DriverError

      # The message is constant.
      #
      # @since 2.0.0
      MESSAGE = 'nil is an invalid collection name. ' +
        'Please provide a string or symbol.'

      # Instantiate the new exception.
      #
      # @example Instantiate the exception.
      #   Mongo::Collection::InvalidName.new
      #
      # @since 2.0.0
      def initialize(message=MESSAGE)
        super(message)
      end
    end

    private

    # Add a primary key to a document, using either a custom primary key factory or
    # BSON::ObjectId.new
    #
    # @param [ Hash ] doc A document.
    #
    # @return [ Hash ] the altered document.
    #
    # @since 2.0.0
    def add_pk!(doc)
      if @pk_factory
        @pk_factory.create_pk(doc)
      else
        doc.merge({ :_id => BSON::ObjectId.new })
      end
    end

    # Run certain validations on options passed in by a user and raise
    # exceptions when appropriate.
    #
    # @param [ Hash ] opts Options.
    #
    # @since 2.0.0
    def validate_opts(opts={})
      if opts[:fsync] && opts[:j]
        raise Mongo::ArgumentError, "cannot use fsync in combination with j option"
      end
    end

    # Get a server context for this operation.
    #
    # @param [ Hash ] opts Options from the query.
    # @param [ true, false ] primary Does this operation need to use a primary?
    #
    # @return [ Context ] a context object.
    #
    # @since 2.0.0
    def get_context(opts, primary=false)
      if primary
        server_preference(opts).primary(cluster.servers).first.context
      else
        server_preference(opts).select_servers(cluster.servers).first.context
      end
    end

    # Return the proper write concern for this operation.
    #
    # @param [ Hash ] opts Options.
    #
    # @since 2.0.0
    def write_concern(opts={})
      write_concern = opts[:w] ? WriteConcern::Mode.get(opts[:w]) :
        database.write_concern
    end
  end
end

# --
# Copyright (C) 2008-2009 10gen Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ++

module Mongo

  # A named collection of documents in a database.
  class Collection

    attr_reader :db, :name, :pk_factory, :hint

    # Initialize a collection object.
    #
    # @param [DB] db a MongoDB database instance.
    # @param [String, Symbol] name the name of the collection.
    #
    # @raise [InvalidName]
    #   if collection name is empty, contains '$', or starts or ends with '.'
    #
    # @raise [TypeError]
    #   if collection name is not a string or symbol
    #
    # @return [Collection]
    def initialize(db, name, pk_factory=nil)
      case name
      when Symbol, String
      else
        raise TypeError, "new_name must be a string or symbol"
      end

      name = name.to_s

      if name.empty? or name.include? ".."
        raise InvalidName, "collection names cannot be empty"
      end
      if name.include? "$"
        raise InvalidName, "collection names must not contain '$'" unless name =~ /((^\$cmd)|(oplog\.\$main))/
      end
      if name.match(/^\./) or name.match(/\.$/)
        raise InvalidName, "collection names must not start or end with '.'"
      end

      @db, @name  = db, name
      @connection = @db.connection
      @pk_factory = pk_factory || ObjectID
      @hint = nil
    end

    # Return a sub-collection of this collection by name. If 'users' is a collection, then
    # 'users.comments' is a sub-collection of users.
    #
    # @param [String] name
    #   the collection to return
    #
    # @raise [InvalidName]
    #   if passed an invalid collection name
    #
    # @return [Collection]
    #   the specified sub-collection
    def [](name)
      name = "#{self.name}.#{name}"
      return Collection.new(db, name) if !db.strict? || db.collection_names.include?(name)
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
    #   document to be included in the result set.
    #
    # @option opts [Array] :fields field names that should be returned in the result
    #   set ("_id" will always be included). By limiting results to a certain subset of fields,
    #   you can cut down on network traffic and decoding time.
    # @option opts [Integer] :skip number of documents to skip from the beginning of the result set
    # @option opts [Integer] :limit maximum number of documents to return
    # @option opts [Array]   :sort an array of [key, direction] pairs to sort by. Direction should
    #   be specified as Mongo::ASCENDING (or :ascending / :asc) or Mongo::DESCENDING (or :descending / :desc)
    # @option opts [String, Array, OrderedHash] :hint hint for query optimizer, usually not necessary if using MongoDB > 1.1
    # @option opts [Boolean] :snapshot ('false') if true, snapshot mode will be used for this query.
    #   Snapshot mode assures no duplicates are returned, or objects missed, which were preset at both the start and
    #   end of the query's execution. For details see http://www.mongodb.org/display/DOCS/How+to+do+Snapshotting+in+the+Mongo+Database
    # @option opts [Boolean] :timeout ('true') when +true+, the returned cursor will be subject to
    #   the normal cursor timeout behavior of the mongod process. When +false+, the returned cursor will never timeout. Note
    #   that disabling timeout will only work when #find is invoked with a block. This is to prevent any inadvertant failure to
    #   close the cursor, as the cursor is explicitly closed when block code finishes.
    #
    # @raise [ArgumentError]
    #   if timeout is set to false and find is not invoked in a block
    #
    # @raise [RuntimeError]
    #   if given unknown options
    def find(selector={}, opts={})
      fields = opts.delete(:fields)
      fields = ["_id"] if fields && fields.empty?
      skip   = opts.delete(:skip) || skip || 0
      limit  = opts.delete(:limit) || 0
      sort   = opts.delete(:sort)
      hint   = opts.delete(:hint)
      snapshot = opts.delete(:snapshot)
      if opts[:timeout] == false && !block_given?
        raise ArgumentError, "Timeout can be set to false only when #find is invoked with a block."
      end
      timeout = block_given? ? (opts.delete(:timeout) || true) : true
      if hint
        hint = normalize_hint_fields(hint)
      else
        hint = @hint        # assumed to be normalized already
      end
      raise RuntimeError, "Unknown options [#{opts.inspect}]" unless opts.empty?

      cursor = Cursor.new(self, :selector => selector, :fields => fields, :skip => skip, :limit => limit,
                          :order => sort, :hint => hint, :snapshot => snapshot, :timeout => timeout)
      if block_given?
        yield cursor
        cursor.close()
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
    # @param [Hash, ObjectID, Nil] spec_or_object_id a hash specifying elements 
    #   which must be present for a document to be included in the result set or an 
    #   instance of ObjectID to be used as the value for an _id query.
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
             when ObjectID
               {:_id => spec_or_object_id}
             when Hash
               spec_or_object_id
             else
               raise TypeError, "spec_or_object_id must be an instance of ObjectID or Hash, or nil"
             end
      find(spec, opts.merge(:limit => -1)).next_document
    end

    # Save a document to this collection.
    #
    # @param [Hash] doc
    #   the document to be saved. If the document already has an '_id' key,
    #   then an update (upsert) operation will be performed, and any existing
    #   document with that _id is overwritten. Otherwise an insert operation is performed.
    #
    # @return [ObjectID] the _id of the saved document.
    #
    # @option opts [Boolean] :safe (+false+) 
    #   If true, check that the save succeeded. OperationFailure
    #   will be raised on an error. Note that a safe check requires an extra
    #   round-trip to the database.
    def save(doc, options={})
      if doc.has_key?(:_id) || doc.has_key?('_id')
        id = doc[:_id] || doc['_id']
        update({:_id => id}, doc, :upsert => true, :safe => options.delete(:safe))
        id
      else
        insert(doc, :safe => options.delete(:safe))
      end
    end

    # Insert one or more documents into the collection.
    #
    # @param [Hash, Array] doc_or_docs
    #   a document (as a hash) or array of documents to be inserted.
    #
    # @return [ObjectID, Array]
    #   the _id of the inserted document or a list of _ids of all inserted documents.
    #   Note: the object may have been modified by the database's PK factory, if it has one.
    #
    # @option opts [Boolean] :safe (+false+) 
    #   If true, check that the save succeeded. OperationFailure
    #   will be raised on an error. Note that a safe check requires an extra
    #   round-trip to the database.
    def insert(doc_or_docs, options={})
      doc_or_docs = [doc_or_docs] unless doc_or_docs.is_a?(Array)
      doc_or_docs.collect! { |doc| @pk_factory.create_pk(doc) }
      result = insert_documents(doc_or_docs, @name, true, options[:safe])
      result.size > 1 ? result : result.first
    end
    alias_method :<<, :insert

    # Remove all documents from this collection.
    #
    # @param [Hash] selector
    #   If specified, only matching documents will be removed.
    #
    # @option opts [Boolean] :safe [false] run the operation in safe mode, which
    #   will call :getlasterror on the database and report any assertions.
    #
    # @example remove all documents from the 'users' collection:
    #   users.remove
    #   users.remove({})
    #
    # @example remove only documents that have expired:
    #   users.remove({:expire => {"$lte" => Time.now}})
    #
    # @return [True]
    #
    # @raise [Mongo::OperationFailure] an exception will be raised iff safe mode is enabled
    #   and the operation fails.
    def remove(selector={}, opts={})
      # Initial byte is 0.
      message = ByteBuffer.new([0, 0, 0, 0])
      BSON_RUBY.serialize_cstr(message, "#{@db.name}.#{@name}")
      message.put_int(0)
      message.put_array(BSON.serialize(selector, false).to_a)

      if opts[:safe]
        @connection.send_message_with_safe_check(Mongo::Constants::OP_DELETE, message,
          "db.#{@db.name}.remove(#{selector.inspect})")
        # the return value of send_message_with_safe_check isn't actually meaningful --
        # only the fact that it didn't raise an error is -- so just return true
        true
      else
        @connection.send_message(Mongo::Constants::OP_DELETE, message,
          "db.#{@db.name}.remove(#{selector.inspect})")
      end
    end

    # Update a single document in this collection.
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
    # @option [Boolean] :upsert (+false+) if true, performs an upsert (update or insert)
    # @option [Boolean] :multi (+false+) update all documents matching the selector, as opposed to
    #   just the first matching document. Note: only works in MongoDB 1.1.3 or later.
    # @option opts [Boolean] :safe (+false+) 
    #   If true, check that the save succeeded. OperationFailure
    #   will be raised on an error. Note that a safe check requires an extra
    #   round-trip to the database.
    def update(selector, document, options={})
      # Initial byte is 0.
      message = ByteBuffer.new([0, 0, 0, 0])
      BSON_RUBY.serialize_cstr(message, "#{@db.name}.#{@name}")
      update_options  = 0
      update_options += 1 if options[:upsert]
      update_options += 2 if options[:multi]
      message.put_int(update_options)
      message.put_array(BSON.serialize(selector, false).to_a)
      message.put_array(BSON.serialize(document, false).to_a)
      if options[:safe]
        @connection.send_message_with_safe_check(Mongo::Constants::OP_UPDATE, message, @db.name,
          "db.#{@name}.update(#{selector.inspect}, #{document.inspect})")
      else
        @connection.send_message(Mongo::Constants::OP_UPDATE, message,
          "db.#{@name}.update(#{selector.inspect}, #{document.inspect})")
      end
    end

    # Create a new index.
    #
    # @param [String, Array] field_or_spec
    #   should be either a single field name or an array of
    #   [field name, direction] pairs. Directions should be specified as Mongo::ASCENDING or Mongo::DESCENDING.
    #
    # @param [Boolean] unique if true, this index will enforce a uniqueness constraint.
    #
    # @return [String] the name of the index created.
    def create_index(field_or_spec, unique=false)
      field_h = OrderedHash.new
      if field_or_spec.is_a?(String) || field_or_spec.is_a?(Symbol)
        field_h[field_or_spec.to_s] = 1
      else
        field_or_spec.each { |f| field_h[f[0].to_s] = f[1] }
      end
      name = generate_index_names(field_h)
      sel  = {
        :name   => name,
        :ns     => "#{@db.name}.#{@name}",
        :key    => field_h,
        :unique => unique }
      insert_documents([sel], Mongo::DB::SYSTEM_INDEX_COLLECTION, false)
      name
    end

    # Drop a specified index.
    #
    # @param [String] name
    def drop_index(name)
      @db.drop_index(@name, name)
    end

    # Drop all indexes.
    def drop_indexes

      # Note: calling drop_indexes with no args will drop them all.
      @db.drop_index(@name, '*')

    end

    # Drop the entire collection. USE WITH CAUTION.
    def drop
      @db.drop_collection(@name)
    end

    # Perform a map/reduce operation on the current collection.
    #
    # @param [String, Code] map a map function, written in JavaScript.
    # @param [String, Code] reduce a reduce function, written in JavaScript.
    #
    # @option opts [Hash] :query ({}) a query selector document, like what's passed to #find, to limit
    #   the operation to a subset of the collection.
    # @option opts [Array] :sort ([]) an array of [key, direction] pairs to sort by. Direction should
    #   be specified as Mongo::ASCENDING (or :ascending / :asc) or Mongo::DESCENDING (or :descending / :desc)
    # @option opts [Integer] :limit (nil) if passing a query, number of objects to return from the collection.
    # @option opts [String, Code] :finalize (nil) a javascript function to apply to the result set after the
    #   map/reduce operation has finished.
    # @option opts [String] :out (nil) the name of the output collection. If specified, the collection will not be treated as temporary.
    # @option opts [Boolean] :keeptemp (false) if true, the generated collection will be persisted. default is false.
    # @option opts [Boolean ] :verbose (false) if true, provides statistics on job execution time.
    #
    # @return [Collection] a collection containing the results of the operation.
    #
    # @see http://www.mongodb.org/display/DOCS/MapReduce Offical MongoDB map/reduce documentation.
    def map_reduce(map, reduce, opts={})
      map    = Code.new(map) unless map.is_a?(Code)
      reduce = Code.new(reduce) unless reduce.is_a?(Code)

      hash = OrderedHash.new
      hash['mapreduce'] = self.name
      hash['map'] = map
      hash['reduce'] = reduce
      hash.merge! opts

      result = @db.command(hash)
      unless result["ok"] == 1
        raise Mongo::OperationFailure, "map-reduce failed: #{result['errmsg']}"
      end
      @db[result["result"]]
    end
    alias :mapreduce :map_reduce

    # Perform a group aggregation.
    #
    # @param [Array, String, Code, Nil] :key either 1) an array of fields to group by,
    #   2) a javascript function to generate the key object, or 3) nil.
    # @param [Hash] condition an optional document specifying a query to limit the documents over which group is run.
    # @param [Hash] initial initial value of the aggregation counter object
    # @param [String, Code] reduce aggregation function, in JavaScript
    # @param [String, Code] finalize :: optional. a JavaScript function that receives and modifies
    #              each of the resultant grouped objects. Available only when group is run
    #              with command set to true.
    # @param [Boolean] command if true, run the group as a command instead of in an
    #   eval. Note: Running group as eval has been DEPRECATED.
    #
    # @return [Array] the grouped items.
    def group(key, condition, initial, reduce, command=false, finalize=nil)

      if command

        reduce = Code.new(reduce) unless reduce.is_a?(Code)

        group_command = {
          "group" => {
            "ns"      => @name,
            "$reduce" => reduce,
            "cond"    => condition,
            "initial" => initial
          }
        }

        unless key.nil?
          if key.is_a? Array
            key_type = "key"
            key_value = {}
            key.each { |k| key_value[k] = 1 }
          else
            key_type  = "$keyf"
            key_value = key.is_a?(Code) ? key : Code.new(key)
          end

          group_command["group"][key_type] = key_value
        end

        # only add finalize if specified
        if finalize
          finalize = Code.new(finalize) unless finalize.is_a?(Code)
          group_command['group']['finalize'] = finalize
        end

        result = @db.command group_command

        if result["ok"] == 1
          return result["retval"]
        else
          raise OperationFailure, "group command failed: #{result['errmsg']}"
        end

      else

        warn "Collection#group must now be run as a command; you can do this by passing 'true' as the command argument."

        raise OperationFailure, ":finalize can be specified only when " +
          "group is run as a command (set command param to true)" if finalize

        raise OperationFailure, "key must be an array of fields to group by. If you want to pass a key function, 
          run group as a command by passing 'true' as the command argument." unless key.is_a? Array || key.nil?

        case reduce
        when Code
          scope = reduce.scope
        else
          scope = {}
        end
        scope.merge!({
                       "ns" => @name,
                       "keys" => key,
                       "condition" => condition,
                       "initial" => initial })

      group_function = <<EOS
function () {
    var c = db[ns].find(condition);
    var map = new Map();
    var reduce_function = #{reduce};
    while (c.hasNext()) {
        var obj = c.next();

        var key = {};
        for (var i = 0; i < keys.length; i++) {
            var k = keys[i];
            key[k] = obj[k];
        }

        var aggObj = map.get(key);
        if (aggObj == null) {
            var newObj = Object.extend({}, key);
            aggObj = Object.extend(newObj, initial);
            map.put(key, aggObj);
        }
        reduce_function(obj, aggObj);
    }
    return {"result": map.values()};
}
EOS
        @db.eval(Code.new(group_function, scope))["result"]
      end
    end

    # Return a list of distinct values for +key+ across all
    # documents in the collection. The key may use dot notation
    # to reach into an embedded object.
    #
    # @param [String, Symbol, OrderedHash] key or hash to group by.
    # @param [Hash] query a selector for limiting the result set over which to group.
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
    def distinct(key, query=nil)
      raise MongoArgumentError unless [String, Symbol].include?(key.class)
      command = OrderedHash.new
      command[:distinct] = @name
      command[:key]      = key.to_s
      command[:query]    = query

      @db.command(command)["values"]
    end

    # Rename this collection.
    #
    # Note: If operating in auth mode, the client must be authorized as an admin to
    # perform this operation. 
    #
    # @param [String ] new_name the new name for this collection
    #
    # @raise [InvalidName] if +new_name+ is an invalid collection name.
    def rename(new_name)
      case new_name
      when Symbol, String
      else
        raise TypeError, "new_name must be a string or symbol"
      end

      new_name = new_name.to_s

      if new_name.empty? or new_name.include? ".."
        raise InvalidName, "collection names cannot be empty"
      end
      if new_name.include? "$"
        raise InvalidName, "collection names must not contain '$'"
      end
      if new_name.match(/^\./) or new_name.match(/\.$/)
        raise InvalidName, "collection names must not start or end with '.'"
      end

      @db.rename_collection(@name, new_name)
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

    # Get the number of documents in this collection.
    #
    # @return [Integer]
    def count
      find().count()
    end

    alias :size :count

    protected

    def normalize_hint_fields(hint)
      case hint
      when String
        {hint => 1}
      when Hash
        hint
      when nil
        nil
      else
        h = OrderedHash.new
        hint.to_a.each { |k| h[k] = 1 }
        h
      end
    end

    private

    # Sends a Mongo::Constants::OP_INSERT message to the database.
    # Takes an array of +documents+, an optional +collection_name+, and a
    # +check_keys+ setting.
    def insert_documents(documents, collection_name=@name, check_keys=true, safe=false)
      # Initial byte is 0.
      message = ByteBuffer.new([0, 0, 0, 0])
      BSON_RUBY.serialize_cstr(message, "#{@db.name}.#{collection_name}")
      documents.each { |doc| message.put_array(BSON.serialize(doc, check_keys).to_a) }
      if safe
        @connection.send_message_with_safe_check(Mongo::Constants::OP_INSERT, message, @db.name,
          "db.#{collection_name}.insert(#{documents.inspect})")
      else
        @connection.send_message(Mongo::Constants::OP_INSERT, message,
          "db.#{collection_name}.insert(#{documents.inspect})")
      end
      documents.collect { |o| o[:_id] || o['_id'] }
    end

    def generate_index_names(spec)
      indexes = []
      spec.each_pair do |field, direction|
        indexes.push("#{field}_#{direction}")
      end
      indexes.join("_")
    end
  end

end

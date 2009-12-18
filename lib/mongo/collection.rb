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

  # A named collection of records in a database.
  class Collection

    attr_reader :db, :name, :pk_factory, :hint

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

    # Get a sub-collection of this collection by name.
    #
    # Raises InvalidName if an invalid collection name is used.
    #
    # :name :: the name of the collection to get
    def [](name)
      name = "#{self.name}.#{name}"
      return Collection.new(db, name) if !db.strict? || db.collection_names.include?(name)
      raise "Collection #{name} doesn't exist. Currently in strict mode."
    end

    # Set hint fields to use and return +self+. hint may be a single field
    # name, array of field names, or a hash (preferably an OrderedHash).
    # May be +nil+.
    def hint=(hint)
      @hint = normalize_hint_fields(hint)
      self
    end

    # Query the database.
    #
    # The +selector+ argument is a prototype document that all results must
    # match. For example:
    #
    # collection.find({"hello" => "world"})
    #
    # only matches documents that have a key "hello" with value "world".
    # Matches can have other keys *in addition* to "hello".
    #
    # If given an optional block +find+ will yield a Cursor to that block,
    # close the cursor, and then return nil. This guarantees that partially
    # evaluated cursors will be closed. If given no block +find+ returns a
    # cursor.
    #
    # :selector :: A document (hash) specifying elements which must be
    #              present for a document to be included in the result set.
    #
    # Options:
    # :fields :: Array of field names that should be returned in the result
    #            set ("_id" will always be included). By limiting results
    #            to a certain subset of fields you can cut down on network
    #            traffic and decoding time.
    # :skip :: Number of documents to omit (from the start of the result set)
    #          when returning the results
    # :limit :: Maximum number of records to return
    # :sort :: An array of [key, direction] pairs to sort by. Direction should
    #          be specified as Mongo::ASCENDING (or :ascending / :asc) or
    #          Mongo::DESCENDING (or :descending / :desc)
    # :hint :: See #hint. This option overrides the collection-wide value.
    # :snapshot :: If true, snapshot mode will be used for this query.
    #              Snapshot mode assures no duplicates are returned, or
    #              objects missed, which were preset at both the start and
    #              end of the query's execution. For details see
    #              http://www.mongodb.org/display/DOCS/How+to+do+Snapshotting+in+the+Mongo+Database
    # :timeout :: When +true+ (default), the returned cursor will be subject to
    #             the normal cursor timeout behavior of the mongod process.
    #             When +false+, the returned cursor will never timeout. Note
    #             that disabling timeout will only work when #find is invoked
    #             with a block. This is to prevent any inadvertant failure to
    #             close the cursor, as the cursor is explicitly closed when
    #             block code finishes.
    def find(selector={}, options={})
      fields = options.delete(:fields)
      fields = ["_id"] if fields && fields.empty?
      skip   = options.delete(:skip) || skip || 0
      limit  = options.delete(:limit) || 0
      sort   = options.delete(:sort)
      hint   = options.delete(:hint)
      snapshot = options.delete(:snapshot)
      if options[:timeout] == false && !block_given?
        raise ArgumentError, "Timeout can be set to false only when #find is invoked with a block."
      end
      timeout = block_given? ? (options.delete(:timeout) || true) : true
      if hint
        hint = normalize_hint_fields(hint)
      else
        hint = @hint        # assumed to be normalized already
      end
      raise RuntimeError, "Unknown options [#{options.inspect}]" unless options.empty?

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

    # Get a single object from the database.
    #
    # Raises TypeError if the argument is of an improper type. Returns a
    # single document (hash), or nil if no result is found.
    #
    # :spec_or_object_id :: a hash specifying elements which must be
    #   present for a document to be included in the result set OR an
    #   instance of ObjectID to be used as the value for an _id query.
    #   if nil an empty spec, {}, will be used.
    # :options :: options, as passed to Collection#find
    def find_one(spec_or_object_id=nil, options={})
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
      find(spec, options.merge(:limit => -1)).next_document
    end

    # Save a document in this collection.
    #
    # If +to_save+ already has an '_id' then an update (upsert) operation
    # is performed and any existing document with that _id is overwritten.
    # Otherwise an insert operation is performed. Returns the _id of the
    # saved document.
    #
    # :to_save :: the document (a hash) to be saved
    #
    # Options:
    # :safe :: if true, check that the save succeeded. OperationFailure
    #   will be raised on an error. Checking for safety requires an extra
    #   round-trip to the database
    def save(to_save, options={})
      if to_save.has_key?(:_id) || to_save.has_key?('_id')
        id = to_save[:_id] || to_save['_id']
        update({:_id => id}, to_save, :upsert => true, :safe => options.delete(:safe))
        id
      else
        insert(to_save, :safe => options.delete(:safe))
      end
    end

    # Insert a document(s) into this collection.
    #
    # "<<" is aliased to this method. Returns the _id of the inserted
    # document or a list of _ids of the inserted documents. The object(s)
    # may have been modified by the database's PK factory, if it has one.
    #
    # :doc_or_docs :: a document (as a hash) or Array of documents to be
    #   inserted
    #
    # Options:
    # :safe :: if true, check that the insert succeeded. OperationFailure
    #   will be raised on an error. Checking for safety requires an extra
    #   round-trip to the database
    def insert(doc_or_docs, options={})
      doc_or_docs = [doc_or_docs] unless doc_or_docs.is_a?(Array)
      doc_or_docs.collect! { |doc| @pk_factory.create_pk(doc) }
      result = insert_documents(doc_or_docs, @name, true, options[:safe])
      result.size > 1 ? result : result.first
    end
    alias_method :<<, :insert

    # Remove all records from this collection.
    # If +selector+ is specified, only matching documents will be removed.
    #
    # Remove all records from the collection:
    #   @collection.remove
    #   @collection.remove({})
    #
    # Remove only records that have expired:
    #   @collection.remove({:expire => {'$lte' => Time.now}})
    def remove(selector={})
      message = ByteBuffer.new
      message.put_int(0)
      BSON_RUBY.serialize_cstr(message, "#{@db.name}.#{@name}")
      message.put_int(0)
      message.put_array(BSON.serialize(selector, false).unpack("C*"))
      @connection.send_message(Mongo::Constants::OP_DELETE, message,
        "db.#{@db.name}.remove(#{selector.inspect})")
    end

    # Update a single document in this collection.
    #
    # :selector :: a hash specifying elements which must be present for a document to be updated. Note:
    # the update command currently updates only the first document matching the
    # given selector. If you want all matching documents to be updated, be sure
    # to specify :multi => true.
    # :document :: a hash specifying the fields to be changed in the
    # selected document, or (in the case of an upsert) the document to
    # be inserted
    #
    # Options:
    # :upsert :: if true, perform an upsert operation
    # :multi :: update all documents matching the selector, as opposed to
    # just the first matching document. Note: only works in 1.1.3 or later.
    # :safe :: if true, check that the update succeeded. OperationFailure
    # will be raised on an error. Checking for safety requires an extra
    # round-trip to the database
    def update(selector, document, options={})
      message = ByteBuffer.new
      message.put_int(0)
      BSON_RUBY.serialize_cstr(message, "#{@db.name}.#{@name}")
      update_options  = 0
      update_options += 1 if options[:upsert]
      update_options += 2 if options[:multi]
      message.put_int(update_options)
      message.put_array(BSON.serialize(selector, false).unpack("C*"))
      message.put_array(BSON.serialize(document, false).unpack("C*"))
      if options[:safe]
        @connection.send_message_with_safe_check(Mongo::Constants::OP_UPDATE, message, @db.name,
          "db.#{@name}.update(#{selector.inspect}, #{document.inspect})")
      else
        @connection.send_message(Mongo::Constants::OP_UPDATE, message,
          "db.#{@name}.update(#{selector.inspect}, #{document.inspect})")
      end
    end

    # Create a new index. +field_or_spec+
    # should be either a single field name or a Array of [field name,
    # direction] pairs. Directions should be specified as
    # Mongo::ASCENDING or Mongo::DESCENDING.
    # +unique+ is an optional boolean indicating whether this index
    # should enforce a uniqueness constraint.
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

    # Drop index +name+.
    def drop_index(name)
      @db.drop_index(@name, name)
    end

    # Drop all indexes.
    def drop_indexes
      # just need to call drop indexes with no args; will drop them all
      @db.drop_index(@name, '*')
    end

    # Drop the entire collection. USE WITH CAUTION.
    def drop
      @db.drop_collection(@name)
    end

    # Performs a map/reduce operation on the current collection. Returns a new
    # collection containing the results of the operation.
    #
    # Required:
    # +map+    :: a map function, written in javascript.
    # +reduce+ :: a reduce function, written in javascript.
    #
    # Optional:
    # :query    :: a query selector document, like what's passed to #find, to limit
    #              the operation to a subset of the collection.
    # :sort     :: sort parameters passed to the query.
    # :limit    :: number of objects to return from the collection.
    # :finalize :: a javascript function to apply to the result set after the
    #              map/reduce operation has finished.
    # :out      :: the name of the output collection. if specified, the collection will not be treated as temporary.
    # :keeptemp :: if true, the generated collection will be persisted. default is false.
    # :verbose  :: if true, provides statistics on job execution time.
    #
    # For more information on using map/reduce, see http://www.mongodb.org/display/DOCS/MapReduce
    def map_reduce(map, reduce, options={})
      map    = Code.new(map) unless map.is_a?(Code)
      reduce = Code.new(reduce) unless reduce.is_a?(Code)

      hash = OrderedHash.new
      hash['mapreduce'] = self.name
      hash['map'] = map
      hash['reduce'] = reduce
      hash.merge! options

      result = @db.command(hash)
      unless result["ok"] == 1
        raise Mongo::OperationFailure, "map-reduce failed: #{result['errmsg']}"
      end
      @db[result["result"]]
    end
    alias :mapreduce :map_reduce

    # Performs a group query, similar to the 'SQL GROUP BY' operation.
    # Returns an array of grouped items.
    #
    # :key :: either 1) an array of fields to group by, 2) a javascript function to generate
    #         the key object, or 3) nil.
    # :condition :: an optional document specifying a query to limit the documents over which group is run.
    # :initial :: initial value of the aggregation counter object
    # :reduce :: aggregation function as a JavaScript string
    # :finalize :: optional. a JavaScript function that receives and modifies
    #              each of the resultant grouped objects. Available only when group is run
    #              with command set to true.
    # :command :: if true, run the group as a command instead of in an
    #             eval - it is likely that this option will eventually be
    #             deprecated and all groups will be run as commands
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

    # Returns a list of distinct values for +key+ across all
    # documents in the collection. The key may use dot notation
    # to reach into an embedded object.
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
    # You may also pass a document selector as the second parameter
    # to limit the documents over which distinct is run:
    #   @collection.distinct("name.age", {"name.age" => {"$gt" => 24}})
    #     [27]
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
    # If operating in auth mode, client must be authorized as an admin to
    # perform this operation. Raises +InvalidName+ if +new_name+ is an invalid
    # collection name.
    #
    # :new_name :: new name for this collection
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

    # Get information on the indexes for the collection +collection_name+.
    # Returns a hash where the keys are index names (as returned by
    # Collection#create_index and the values are lists of [key, direction]
    # pairs specifying the index (as passed to Collection#create_index).
    def index_information
      @db.index_information(@name)
    end

    # Return a hash containing options that apply to this collection.
    # 'create' will be the collection name. For the other possible keys
    # and values, see DB#create_collection.
    def options
      @db.collections_info(@name).next_document['options']
    end

    # Get the number of documents in this collection.
    def count()
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
      message = ByteBuffer.new
      message.put_int(0)
      BSON_RUBY.serialize_cstr(message, "#{@db.name}.#{collection_name}")
      documents.each { |doc| message.put_array(BSON.serialize(doc, check_keys).unpack("C*")) }
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

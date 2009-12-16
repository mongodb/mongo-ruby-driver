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

require 'socket'
require 'timeout'
require 'digest/md5'
require 'thread'

module Mongo

  # A Mongo database.
  class DB

    SYSTEM_NAMESPACE_COLLECTION = "system.namespaces"
    SYSTEM_INDEX_COLLECTION = "system.indexes"
    SYSTEM_PROFILE_COLLECTION = "system.profile"
    SYSTEM_USER_COLLECTION = "system.users"
    SYSTEM_COMMAND_COLLECTION = "$cmd"

    # Counter for generating unique request ids.
    @@current_request_id = 0

    # Strict mode enforces collection existence checks. When +true+,
    # asking for a collection that does not exist, or trying to create a
    # collection that already exists, raises an error.
    #
    # Strict mode is disabled by default, but enabled (+true+) at any time.
    attr_writer :strict

    # Returns the value of the +strict+ flag.
    def strict?; @strict; end

    # The name of the database.
    attr_reader :name

    # The Mongo::Connection instance connecting to the MongoDB server.
    attr_reader :connection

    # An array of [host, port] pairs.
    attr_reader :nodes

    # The logger instance if :logger is passed to initialize.
    attr_reader :logger

    # The primary key factory object (or +nil+).
    attr_reader :pk_factory

    def pk_factory=(pk_factory)
      raise "error: can not change PK factory" if @pk_factory
      @pk_factory = pk_factory
    end

    # Instances of DB are normally obtained by calling Mongo#db.
    #
    # db_name :: The database name
    #
    # nodes :: An array of [host, port] pairs. See Connection#new, which offers
    #          a more flexible way of defining nodes.
    #
    # options :: A hash of options.
    #
    # Options:
    #
    # :strict :: If true, collections must exist to be accessed and must
    #            not exist to be created. See #collection and #create_collection.
    #
    # :pk :: A primary key factory object that must respond to :create_pk,
    #        which should take a hash and return a hash which merges the
    #        original hash with any primary key fields the factory wishes
    #        to inject. (NOTE: if the object already has a primary key,
    #        the factory should not inject a new key; this means that the
    #        object is being used in a repsert but it already exists.) The
    #        idea here is that when ever a record is inserted, the :pk
    #        object's +create_pk+ method will be called and the new hash
    #        returned will be inserted.
    #
    # :slave_ok :: Only used if +nodes+ contains only one host/port. If
    #              false, when connecting to that host/port we check to
    #              see if the server is the master. If it is not, an error
    #              is thrown.
    #
    # :logger :: Optional Logger instance to which driver usage information
    #            will be logged.
    #
    # :auto_reconnect :: DEPRECATED. See http://www.mongodb.org/display/DOCS/Replica+Pairs+in+Ruby
    def initialize(db_name, connection, options={})
      @name       = validate_db_name(db_name)
      @connection = connection
      @strict     = options[:strict]
      @pk_factory = options[:pk]
    end

    # Returns true if +username+ has +password+ in
    # +SYSTEM_USER_COLLECTION+. +name+ is username, +password+ is
    # plaintext password.
    def authenticate(username, password)
      doc = command(:getnonce => 1)
      raise "error retrieving nonce: #{doc}" unless ok?(doc)
      nonce = doc['nonce']

      auth = OrderedHash.new
      auth['authenticate'] = 1
      auth['user'] = username
      auth['nonce'] = nonce
      auth['key'] = Digest::MD5.hexdigest("#{nonce}#{username}#{hash_password(username, password)}")
      ok?(command(auth))
    end

    # Deauthorizes use for this database for this connection.
    def logout
      doc = command(:logout => 1)
      raise "error logging out: #{doc.inspect}" unless ok?(doc)
    end

    # Returns an array of collection names in this database.
    def collection_names
      names = collections_info.collect { |doc| doc['name'] || '' }
      names = names.delete_if {|name| name.index(@name).nil? || name.index('$')}
      names.map {|name| name.sub(@name + '.', '')}
    end

    # Retruns an array of Collection instances, one for each collection in this
    # database.
    def collections
      collection_names.map do |collection_name|
        Collection.new(self, collection_name)
      end
    end

    # Returns a cursor over query result hashes. Each hash contains a
    # 'name' string and optionally an 'options' hash. If +coll_name+ is
    # specified, an array of length 1 is returned.
    def collections_info(coll_name=nil)
      selector = {}
      selector[:name] = full_collection_name(coll_name) if coll_name
      Cursor.new(Collection.new(self, SYSTEM_NAMESPACE_COLLECTION), :selector => selector)
    end

    # Create a collection. If +strict+ is false, will return existing or
    # new collection. If +strict+ is true, will raise an error if
    # collection +name+ already exists.
    #
    # Options is an optional hash:
    #
    # :capped :: Boolean. If not specified, capped is +false+.
    #
    # :size :: If +capped+ is +true+, specifies the maximum number of
    #          bytes. If +false+, specifies the initial extent of the
    #          collection.
    #
    # :max :: Max number of records in a capped collection. Optional.
    def create_collection(name, options={})
      # First check existence
      if collection_names.include?(name)
        if strict?
          raise "Collection #{name} already exists. Currently in strict mode."
        else
          return Collection.new(self, name)
        end
      end

      # Create new collection
      oh = OrderedHash.new
      oh[:create] = name
      doc = command(oh.merge(options || {}))
      ok = doc['ok']
      return Collection.new(self, name, @pk_factory) if ok.kind_of?(Numeric) && (ok.to_i == 1 || ok.to_i == 0)
      raise "Error creating collection: #{doc.inspect}"
    end

    def admin
      Admin.new(self)
    end

    # Return a collection. If +strict+ is false, will return existing or
    # new collection. If +strict+ is true, will raise an error if
    # collection +name+ does not already exists.
    def collection(name)
      return Collection.new(self, name, @pk_factory) if !strict? || collection_names.include?(name)
      raise "Collection #{name} doesn't exist. Currently in strict mode."
    end
    alias_method :[], :collection

    # Drop collection +name+. Returns +true+ on success or if the
    # collection does not exist, +false+ otherwise.
    def drop_collection(name)
      return true unless collection_names.include?(name)

      ok?(command(:drop => name))
    end

    # Returns the error message from the most recently executed database
    # operation for this connection, or +nil+ if there was no error.
    def error
      doc = command(:getlasterror => 1)
      raise "error retrieving last error: #{doc}" unless ok?(doc)
      doc['err']
    end

    # Get status information from the last operation on this connection.
    def last_status
      command(:getlasterror => 1)
    end

    # Returns +true+ if an error was caused by the most recently executed
    # database operation.
    def error?
      error != nil
    end

    # Get the most recent error to have occured on this database
    #
    # Only returns errors that have occured since the last call to
    # DB#reset_error_history - returns +nil+ if there is no such error.
    def previous_error
      error = command(:getpreverror => 1)
      if error["err"]
        error
      else
        nil
      end
    end

    # Reset the error history of this database
    #
    # Calls to DB#previous_error will only return errors that have occurred
    # since the most recent call to this method.
    def reset_error_history
      command(:reseterror => 1)
    end

    # Returns a Cursor over the query results.
    #
    # Note that the query gets sent lazily; the cursor calls
    # #send_query_message when needed. If the caller never requests an
    # object from the cursor, the query never gets sent.
    def query(collection, query, admin=false)
      Cursor.new(self, collection, query, admin)
    end

    # Dereference a DBRef, getting the document it points to.
    def dereference(dbref)
      collection(dbref.namespace).find_one("_id" => dbref.object_id)
    end

    # Evaluate a JavaScript expression on MongoDB.
    # +code+ should be a string or Code instance containing a JavaScript
    # expression. Additional arguments will be passed to that expression
    # when it is run on the server.
    def eval(code, *args)
      if not code.is_a? Code
        code = Code.new(code)
      end

      oh = OrderedHash.new
      oh[:$eval] = code
      oh[:args] = args
      doc = command(oh)
      return doc['retval'] if ok?(doc)
      raise OperationFailure, "eval failed: #{doc['errmsg']}"
    end

    # Rename collection +from+ to +to+. Meant to be called by
    # Collection#rename.
    def rename_collection(from, to)
      oh = OrderedHash.new
      oh[:renameCollection] = "#{@name}.#{from}"
      oh[:to] = "#{@name}.#{to}"
      doc = command(oh, true)
      raise "Error renaming collection: #{doc.inspect}" unless ok?(doc)
    end

    # Drop index +name+ from +collection_name+. Normally called from
    # Collection#drop_index or Collection#drop_indexes.
    def drop_index(collection_name, name)
      oh = OrderedHash.new
      oh[:deleteIndexes] = collection_name
      oh[:index] = name
      doc = command(oh)
      raise "Error with drop_index command: #{doc.inspect}" unless ok?(doc)
    end

    # Get information on the indexes for the collection +collection_name+.
    # Normally called by Collection#index_information. Returns a hash where
    # the keys are index names (as returned by Collection#create_index and
    # the values are lists of [key, direction] pairs specifying the index
    # (as passed to Collection#create_index).
    def index_information(collection_name)
      sel = {:ns => full_collection_name(collection_name)}
      info = {}
      Cursor.new(Collection.new(self, SYSTEM_INDEX_COLLECTION), :selector => sel).each { |index|
        info[index['name']] = index['key'].map {|k| k}
      }
      info
    end

    # Create a new index on +collection_name+. +field_or_spec+
    # should be either a single field name or a Array of [field name,
    # direction] pairs. Directions should be specified as
    # Mongo::ASCENDING or Mongo::DESCENDING. Normally called
    # by Collection#create_index. If +unique+ is true the index will
    # enforce a uniqueness constraint.
    def create_index(collection_name, field_or_spec, unique=false)
      self.collection(collection_name).create_index(field_or_spec, unique)
    end

    # Return +true+ if +doc+ contains an 'ok' field with the value 1.
    def ok?(doc)
      ok = doc['ok']
      ok.kind_of?(Numeric) && ok.to_i == 1
    end

    # DB commands need to be ordered, so selector must be an OrderedHash
    # (or a Hash with only one element). What DB commands really need is
    # that the "command" key be first.
    def command(selector, use_admin_db=false, sock=nil)
      if !selector.kind_of?(OrderedHash)
        if !selector.kind_of?(Hash) || selector.keys.length > 1
          raise "command must be given an OrderedHash when there is more than one key"
        end
      end

      cursor = Cursor.new(Collection.new(self, SYSTEM_COMMAND_COLLECTION), :admin => use_admin_db, :limit => -1, :selector => selector, :socket => sock)
      cursor.next_document
    end

    # Sends a command to the database.
    #
    # :selector (required) :: An OrderedHash, or a standard Hash with just one
    # key, specifying the command to be performed.
    #
    # :admin (optional) :: If true, the command will be executed on the admin
    # collection.
    #
    # :check_response (optional) :: If true, will raise an exception if the
    # command fails.
    #
    # Note: DB commands must start with the "command" key. For this reason,
    # any selector containing more than one key must be an OrderedHash.
    def command(selector, admin=false, check_response=false, sock=nil)
      raise MongoArgumentError, "command must be given a selector" unless selector.is_a?(Hash) && !selector.empty?
      if selector.class.eql?(Hash) && selector.keys.length > 1
        raise MongoArgumentError, "DB#command requires an OrderedHash when hash contains multiple keys"
      end

      result = Cursor.new(system_command_collection, :admin => admin,
        :limit => -1, :selector => selector, :socket => sock).next_document

      if check_response && !ok?(result)
        raise OperationFailure, "Database command '#{selector.keys.first}' failed."
      else
        result
      end
    end

    # DEPRECATED: please use DB#command instead.
    def db_command(*args)
      warn "DB#db_command has been DEPRECATED. Please use DB#command instead."
      command(args[0], args[1])
    end

    def full_collection_name(collection_name)
      "#{@name}.#{collection_name}"
    end

    private

    def hash_password(username, plaintext)
      Digest::MD5.hexdigest("#{username}:mongo:#{plaintext}")
    end

    def system_command_collection
      Collection.new(self, SYSTEM_COMMAND_COLLECTION)
    end

    def validate_db_name(db_name)
      unless [String, Symbol].include?(db_name.class)
        raise TypeError, "db_name must be a string or symbol"
      end

      [" ", ".", "$", "/", "\\"].each do |invalid_char|
        if db_name.include? invalid_char
          raise InvalidName, "database names cannot contain the character '#{invalid_char}'"
        end
      end
      raise InvalidName, "database name cannot be the empty string" if db_name.empty?
      db_name
    end
  end
end

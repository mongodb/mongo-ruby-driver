require 'socket'
require 'thread'

module Mongo

  # A MongoDB database.
  class DB
    include Mongo::WriteConcern

    SYSTEM_NAMESPACE_COLLECTION = 'system.namespaces'
    SYSTEM_INDEX_COLLECTION     = 'system.indexes'
    SYSTEM_PROFILE_COLLECTION   = 'system.profile'
    SYSTEM_USER_COLLECTION      = 'system.users'
    SYSTEM_JS_COLLECTION        = 'system.js'
    SYSTEM_COMMAND_COLLECTION   = '$cmd'

    PROFILE_LEVEL = {
      :off       => 0,
      :slow_only => 1,
      :all       => 2
    }

    # Counter for generating unique request ids.
    @@current_request_id = 0

    # Strict mode enforces collection existence checks. When +true+,
    # asking for a collection that does not exist, or trying to create a
    # collection that already exists, raises an error.
    #
    # Strict mode is disabled by default, but enabled (+true+) at any time.
    #
    # @deprecated Support for strict will be removed in version 2.0 of the driver.
    def strict=(value)
      unless ENV['TEST_MODE']
        warn "Support for strict mode has been deprecated and will be " +
             "removed in version 2.0 of the driver."
      end
      @strict = value
    end

    # Returns the value of the +strict+ flag.
    #
    # @deprecated Support for strict will be removed in version 2.0 of the driver.
    def strict?
      @strict
    end

    # The name of the database and the local write concern options.
    attr_reader :name, :write_concern

    # The Mongo::MongoClient instance connecting to the MongoDB server.
    attr_reader :connection

    # The length of time that Collection.ensure_index should cache index calls
    attr_accessor :cache_time

    # Read Preference
    attr_accessor :read, :tag_sets, :acceptable_latency

    # Instances of DB are normally obtained by calling Mongo#db.
    #
    # @param [String] name the database name.
    # @param [Mongo::MongoClient] client a connection object pointing to MongoDB. Note
    #   that databases are usually instantiated via the MongoClient class. See the examples below.
    #
    # @option opts [Boolean] :strict (False) [DEPRECATED] If true, collections existence checks are
    # performed during a number of relevant operations. See DB#collection, DB#create_collection and
    # DB#drop_collection.
    #
    # @option opts [Object, #create_pk(doc)] :pk (BSON::ObjectId) A primary key factory object,
    #   which should take a hash and return a hash which merges the original hash with any primary key
    #   fields the factory wishes to inject. (NOTE: if the object already has a primary key,
    #   the factory should not inject a new key).
    #
    # @option opts [String, Integer, Symbol] :w (1) Set default number of nodes to which a write
    #   should be acknowledged
    # @option opts [Boolean] :j (false) Set journal acknowledgement
    # @option opts [Integer] :wtimeout (nil) Set replica set acknowledgement timeout
    # @option opts [Boolean] :fsync (false) Set fsync acknowledgement.
    #
    #   Notes on write concern:
    #     These write concern options are propagated to Collection objects instantiated off of this DB. If no
    #     options are provided, the default write concern set on this instance's MongoClient object will be used. This
    #     default can be overridden upon instantiation of any collection by explicitly setting write concern options
    #     on initialization or at the time of an operation.
    #
    # @option opts [Integer] :cache_time (300) Set the time that all ensure_index calls should cache the command.
    #
    # @core databases constructor_details
    def initialize(name, client, opts={})
      @name       = Mongo::Support.validate_db_name(name)
      @connection = client
      @strict     = opts[:strict]
      @pk_factory = opts[:pk]

      @write_concern = get_write_concern(opts, client)

      @read = opts[:read] || @connection.read
      Mongo::ReadPreference::validate(@read)
      @tag_sets = opts.fetch(:tag_sets, @connection.tag_sets)
      @acceptable_latency = opts.fetch(:acceptable_latency, @connection.acceptable_latency)
      @cache_time = opts[:cache_time] || 300 #5 minutes.
    end

    # Authenticate with the given username and password. Note that mongod
    # must be started with the --auth option for authentication to be enabled.
    #
    # @param [String] username
    # @param [String] password
    # @param [Boolean] save_auth
    #   Save this authentication to the client object using MongoClient#add_auth. This
    #   will ensure that the authentication will be applied on database reconnect. Note
    #   that this value must be true when using connection pooling.
    #
    # @return [Boolean]
    #
    # @raise [AuthenticationError]
    #
    # @core authenticate authenticate-instance_method
    def authenticate(username, password, save_auth=true)
      if @connection.pool_size > 1 && !save_auth
        raise MongoArgumentError, "If using connection pooling, :save_auth must be set to true."
      end

      begin
        socket = @connection.checkout_reader(:mode => :primary_preferred)
        issue_authentication(username, password, save_auth, :socket => socket)
      ensure
        socket.checkin if socket
      end

      @connection.authenticate_pools
    end

    def issue_authentication(username, password, save_auth=true, opts={})
      doc = command({:getnonce => 1}, :check_response => false, :socket => opts[:socket])
      raise MongoDBError, "Error retrieving nonce: #{doc}" unless ok?(doc)
      nonce = doc['nonce']

      auth = BSON::OrderedHash.new
      auth['authenticate'] = 1
      auth['user'] = username
      auth['nonce'] = nonce
      auth['key'] = Mongo::Support.auth_key(username, password, nonce)
      if ok?(doc = self.command(auth, :check_response => false, :socket => opts[:socket]))
        @connection.add_auth(@name, username, password) if save_auth
      else
        message = "Failed to authenticate user '#{username}' on db '#{self.name}'"
        raise Mongo::AuthenticationError.new(message, doc['code'], doc)
      end
      true
    end

    # Adds a stored Javascript function to the database which can executed
    # server-side in map_reduce, db.eval and $where clauses.
    #
    # @param [String] function_name
    # @param [String] code
    #
    # @return [String] the function name saved to the database
    def add_stored_function(function_name, code)
      self[SYSTEM_JS_COLLECTION].save(
        {
          "_id" => function_name,
          :value => BSON::Code.new(code)
        }
      )
    end

    # Removes stored Javascript function from the database.  Returns
    # false if the function does not exist
    #
    # @param [String] function_name
    #
    # @return [Boolean]
    def remove_stored_function(function_name)
      return false unless self[SYSTEM_JS_COLLECTION].find_one({"_id" => function_name})
      self[SYSTEM_JS_COLLECTION].remove({"_id" => function_name}, :w => 1)
    end

    # Adds a user to this database for use with authentication. If the user already
    # exists in the system, the password will be updated.
    #
    # @param [String] username
    # @param [String] password
    # @param [Boolean] read_only
    #   Create a read-only user.
    #
    # @return [Hash] an object representing the user.
    def add_user(username, password, read_only = false)
      users = self[SYSTEM_USER_COLLECTION]
      user  = users.find_one({:user => username}) || {:user => username}
      user['pwd'] = Mongo::Support.hash_password(username, password)
      user['readOnly'] = true if read_only;
      begin
        users.save(user)
      rescue OperationFailure => ex
        # adding first admin user fails GLE in MongoDB 2.2
        raise ex unless ex.message =~ /login/
      end
      user
    end

    # Remove the given user from this database. Returns false if the user
    # doesn't exist in the system.
    #
    # @param [String] username
    #
    # @return [Boolean]
    def remove_user(username)
      if self[SYSTEM_USER_COLLECTION].find_one({:user => username})
        self[SYSTEM_USER_COLLECTION].remove({:user => username}, :w => 1)
      else
        false
      end
    end

    # Deauthorizes use for this database for this client connection. Also removes
    # any saved authentication in the MongoClient class associated with this
    # database.
    #
    # @raise [MongoDBError] if logging out fails.
    #
    # @return [Boolean]
    def logout(opts={})
      @connection.logout_pools(@name) if @connection.pool_size > 1
      issue_logout(opts)
    end

    def issue_logout(opts={})
      if ok?(doc = command({:logout => 1}, :socket => opts[:socket]))
        @connection.remove_auth(@name)
      else
        raise MongoDBError, "Error logging out: #{doc.inspect}"
      end
      true
    end

    # Get an array of collection names in this database.
    #
    # @return [Array]
    def collection_names
      names = collections_info.collect { |doc| doc['name'] || '' }
      names = names.delete_if {|name| name.index(@name).nil? || name.index('$')}
      names.map {|name| name.sub(@name + '.', '')}
    end

    # Get an array of Collection instances, one for each collection in this database.
    #
    # @return [Array<Mongo::Collection>]
    def collections
      collection_names.map do |name|
        Collection.new(name, self)
      end
    end

    # Get info on system namespaces (collections). This method returns
    # a cursor which can be iterated over. For each collection, a hash
    # will be yielded containing a 'name' string and, optionally, an 'options' hash.
    #
    # @param [String] coll_name return info for the specified collection only.
    #
    # @return [Mongo::Cursor]
    def collections_info(coll_name=nil)
      selector = {}
      selector[:name] = full_collection_name(coll_name) if coll_name
      Cursor.new(Collection.new(SYSTEM_NAMESPACE_COLLECTION, self), :selector => selector)
    end

    # Create a collection.
    #
    # new collection. If +strict+ is true, will raise an error if
    # collection +name+ already exists.
    #
    # @param [String, Symbol] name the name of the new collection.
    #
    # @option opts [Boolean] :capped (False) created a capped collection.
    #
    # @option opts [Integer] :size (Nil) If +capped+ is +true+,
    #   specifies the maximum number of bytes for the capped collection.
    #   If +false+, specifies the number of bytes allocated
    #   for the initial extent of the collection.
    #
    # @option opts [Integer] :max (Nil) If +capped+ is +true+, indicates
    #   the maximum number of records in a capped collection.
    #
    # @raise [MongoDBError] raised under two conditions:
    #   either we're in +strict+ mode and the collection
    #   already exists or collection creation fails on the server.
    #
    # @return [Mongo::Collection]
    def create_collection(name, opts={})
      name = name.to_s
      if strict? && collection_names.include?(name)
        raise MongoDBError, "Collection '#{name}' already exists. (strict=true)"
      end

      begin
        cmd = BSON::OrderedHash.new
        cmd[:create] = name
        doc = command(cmd.merge(opts || {}))
        return Collection.new(name, self, :pk => @pk_factory) if ok?(doc)
      rescue OperationFailure => e
        return Collection.new(name, self, :pk => @pk_factory) if e.message =~ /exists/
        raise e
      end
      raise MongoDBError, "Error creating collection: #{doc.inspect}"
    end

    # Get a collection by name.
    #
    # @param [String, Symbol] name the collection name.
    # @param [Hash] opts any valid options that can be passed to Collection#new.
    #
    # @raise [MongoDBError] if collection does not already exist and we're in
    #   +strict+ mode.
    #
    # @return [Mongo::Collection]
    def collection(name, opts={})
      if strict? && !collection_names.include?(name.to_s)
        raise MongoDBError, "Collection '#{name}' doesn't exist. (strict=true)"
      else
        opts = opts.dup
        opts.merge!(:pk => @pk_factory) unless opts[:pk]
        Collection.new(name, self, opts)
      end
    end
    alias_method :[], :collection

    # Drop a collection by +name+.
    #
    # @param [String, Symbol] name
    #
    # @return [Boolean] +true+ on success or +false+ if the collection name doesn't exist.
    def drop_collection(name)
      return false if strict? && !collection_names.include?(name.to_s)
      begin
        ok?(command(:drop => name))
      rescue OperationFailure => e
        false
      end
    end

    # Run the getlasterror command with the specified replication options.
    #
    # @option opts [Boolean] :fsync (false)
    # @option opts [Integer] :w (nil)
    # @option opts [Integer] :wtimeout (nil)
    # @option opts [Boolean] :j (false)
    #
    # @return [Hash] the entire response to getlasterror.
    #
    # @raise [MongoDBError] if the operation fails.
    def get_last_error(opts={})
      cmd = BSON::OrderedHash.new
      cmd[:getlasterror] = 1
      cmd.merge!(opts)
      doc = command(cmd, :check_response => false)
      raise MongoDBError, "Error retrieving last error: #{doc.inspect}" unless ok?(doc)
      doc
    end

    # Return +true+ if an error was caused by the most recently executed
    # database operation.
    #
    # @return [Boolean]
    def error?
      get_last_error['err'] != nil
    end

    # Get the most recent error to have occurred on this database.
    #
    # This command only returns errors that have occurred since the last call to
    # DB#reset_error_history - returns +nil+ if there is no such error.
    #
    # @return [String, Nil] the text of the error or +nil+ if no error has occurred.
    def previous_error
      error = command(:getpreverror => 1)
      error["err"] ? error : nil
    end

    # Reset the error history of this database
    #
    # Calls to DB#previous_error will only return errors that have occurred
    # since the most recent call to this method.
    #
    # @return [Hash]
    def reset_error_history
      command(:reseterror => 1)
    end

    # Dereference a DBRef, returning the document it points to.
    #
    # @param [Mongo::DBRef] dbref
    #
    # @return [Hash] the document indicated by the db reference.
    #
    # @see http://www.mongodb.org/display/DOCS/DB+Ref MongoDB DBRef spec.
    def dereference(dbref)
      collection(dbref.namespace).find_one("_id" => dbref.object_id)
    end

    # Evaluate a JavaScript expression in MongoDB.
    #
    # @param [String, Code] code a JavaScript expression to evaluate server-side.
    # @param [Integer, Hash] args any additional argument to be passed to the +code+ expression when
    #   it's run on the server.
    #
    # @return [String] the return value of the function.
    def eval(code, *args)
      unless code.is_a?(BSON::Code)
        code = BSON::Code.new(code)
      end

      cmd = BSON::OrderedHash.new
      cmd[:$eval] = code
      cmd[:args] = args
      doc = command(cmd)
      doc['retval']
    end

    # Rename a collection.
    #
    # @param [String] from original collection name.
    # @param [String] to new collection name.
    #
    # @return [True] returns +true+ on success.
    #
    # @raise MongoDBError if there's an error renaming the collection.
    def rename_collection(from, to)
      cmd = BSON::OrderedHash.new
      cmd[:renameCollection] = "#{@name}.#{from}"
      cmd[:to] = "#{@name}.#{to}"
      doc = DB.new('admin', @connection).command(cmd, :check_response => false)
      ok?(doc) || raise(MongoDBError, "Error renaming collection: #{doc.inspect}")
    end

    # Drop an index from a given collection. Normally called from
    # Collection#drop_index or Collection#drop_indexes.
    #
    # @param [String] collection_name
    # @param [String] index_name
    #
    # @return [True] returns +true+ on success.
    #
    # @raise MongoDBError if there's an error dropping the index.
    def drop_index(collection_name, index_name)
      cmd = BSON::OrderedHash.new
      cmd[:deleteIndexes] = collection_name
      cmd[:index] = index_name.to_s
      doc = command(cmd, :check_response => false)
      ok?(doc) || raise(MongoDBError, "Error with drop_index command: #{doc.inspect}")
    end

    # Get information on the indexes for the given collection.
    # Normally called by Collection#index_information.
    #
    # @param [String] collection_name
    #
    # @return [Hash] keys are index names and the values are lists of [key, type] pairs
    #   defining the index.
    def index_information(collection_name)
      sel  = {:ns => full_collection_name(collection_name)}
      info = {}
      Cursor.new(Collection.new(SYSTEM_INDEX_COLLECTION, self), :selector => sel).each do |index|
        info[index['name']] = index
      end
      info
    end

    # Return stats on this database. Uses MongoDB's dbstats command.
    #
    # @return [Hash]
    def stats
      self.command({:dbstats => 1})
    end

    # Return +true+ if the supplied +doc+ contains an 'ok' field with the value 1.
    #
    # @param [Hash] doc
    #
    # @return [Boolean]
    def ok?(doc)
      Mongo::Support.ok?(doc)
    end

    # Send a command to the database.
    #
    # Note: DB commands must start with the "command" key. For this reason,
    # any selector containing more than one key must be an OrderedHash.
    #
    # Note also that a command in MongoDB is just a kind of query
    # that occurs on the system command collection ($cmd). Examine this method's implementation
    # to see how it works.
    #
    # @param [OrderedHash, Hash] selector an OrderedHash, or a standard Hash with just one
    # key, specifying the command to be performed. In Ruby 1.9, OrderedHash isn't necessary since
    # hashes are ordered by default.
    #
    # @option opts [Boolean] :check_response (true) If +true+, raises an exception if the
    #   command fails.
    # @option opts [Socket] :socket a socket to use for sending the command. This is mainly for internal use.
    # @option opts [:primary, :secondary] :read Read preference for this command. See Collection#find for
    #   more details.
    # @option opts [String]  :comment (nil) a comment to include in profiling logs
    #
    # @return [Hash]
    #
    # @core commands command_instance-method
    def command(selector, opts={})
      check_response = opts.fetch(:check_response, true)
      socket = opts[:socket]
      raise MongoArgumentError, "Command must be given a selector" unless selector.is_a?(Hash) && !selector.empty?

      if selector.keys.length > 1 && RUBY_VERSION < '1.9' && selector.class != BSON::OrderedHash
        raise MongoArgumentError, "DB#command requires an OrderedHash when hash contains multiple keys"
      end

      if read_pref = opts[:read]
        Mongo::ReadPreference::validate(read_pref)
        unless read_pref == :primary || Mongo::Support::secondary_ok?(selector)
          raise MongoArgumentError, "Command is not supported on secondaries: #{selector.keys.first}"
        end
      end

      begin
        result = Cursor.new(
          system_command_collection,
          :limit => -1,
          :selector => selector,
          :socket => socket,
          :read => read_pref,
          :comment => opts[:comment]).next_document
      rescue OperationFailure => ex
        raise OperationFailure, "Database command '#{selector.keys.first}' failed: #{ex.message}"
      end

      raise OperationFailure,
        "Database command '#{selector.keys.first}' failed: returned null." unless result

      if check_response && !ok?(result)
        message = "Database command '#{selector.keys.first}' failed: ("
        message << result.map do |key, value|
          "#{key}: '#{value}'"
        end.join('; ')
        message << ').'
        code = result['code'] || result['assertionCode']
        raise OperationFailure.new(message, code, result)
      end

      result
    end

    # A shortcut returning db plus dot plus collection name.
    #
    # @param [String] collection_name
    #
    # @return [String]
    def full_collection_name(collection_name)
      "#{@name}.#{collection_name}"
    end

    # The primary key factory object (or +nil+).
    #
    # @return [Object, Nil]
    def pk_factory
      @pk_factory
    end

    # Specify a primary key factory if not already set.
    #
    # @raise [MongoArgumentError] if the primary key factory has already been set.
    def pk_factory=(pk_factory)
      raise MongoArgumentError,
        "Cannot change primary key factory once it's been set" if @pk_factory

      @pk_factory = pk_factory
    end

    # Return the current database profiling level. If profiling is enabled, you can
    # get the results using DB#profiling_info.
    #
    # @return [Symbol] :off, :slow_only, or :all
    #
    # @core profiling profiling_level-instance_method
    def profiling_level
      cmd = BSON::OrderedHash.new
      cmd[:profile] = -1
      doc = command(cmd, :check_response => false)

      raise "Error with profile command: #{doc.inspect}" unless ok?(doc)

      level_sym = PROFILE_LEVEL.invert[doc['was'].to_i]
      raise "Error: illegal profiling level value #{doc['was']}" unless level_sym
      level_sym
    end

    # Set this database's profiling level. If profiling is enabled, you can
    # get the results using DB#profiling_info.
    #
    # @param [Symbol] level acceptable options are +:off+, +:slow_only+, or +:all+.
    def profiling_level=(level)
      cmd = BSON::OrderedHash.new
      cmd[:profile] = PROFILE_LEVEL[level]
      doc = command(cmd, :check_response => false)
      ok?(doc) || raise(MongoDBError, "Error with profile command: #{doc.inspect}")
    end

    # Get the current profiling information.
    #
    # @return [Array] a list of documents containing profiling information.
    def profiling_info
      Cursor.new(Collection.new(SYSTEM_PROFILE_COLLECTION, self), :selector => {}).to_a
    end

    # Validate a named collection.
    #
    # @param [String] name the collection name.
    #
    # @return [Hash] validation information.
    #
    # @raise [MongoDBError] if the command fails or there's a problem with the validation
    #   data, or if the collection is invalid.
    def validate_collection(name)
      cmd = BSON::OrderedHash.new
      cmd[:validate] = name
      cmd[:full] = true
      doc = command(cmd, :check_response => false)

      raise MongoDBError, "Error with validate command: #{doc.inspect}" unless ok?(doc)

      if (doc.has_key?('valid') && !doc['valid']) || (doc['result'] =~ /\b(exception|corrupt)\b/i)
        raise MongoDBError, "Error: invalid collection #{name}: #{doc.inspect}"
      end
      doc
    end

    private

    def system_command_collection
      Collection.new(SYSTEM_COMMAND_COLLECTION, self)
    end
  end
end

# Copyright (C) 2014-2019 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

module Mongo

  # The client is the entry point to the driver and is the main object that
  # will be interacted with.
  #
  # @since 2.0.0
  class Client
    extend Forwardable
    include Loggable

    # The options that do not affect the behavior of a cluster and its
    # subcomponents.
    #
    # @since 2.1.0
    CRUD_OPTIONS = [ :database, :read, :write, :retry_writes ].freeze

    # Valid client options.
    #
    # @since 2.1.2
    VALID_OPTIONS = [
      :app_name,
      :auth_mech,
      :auth_mech_properties,
      :auth_source,
      :compressors,
      :connect,
      :connect_timeout,
      :database,
      :heartbeat_frequency,
      :id_generator,
      :local_threshold,
      :logger,
      :max_idle_time,
      :max_pool_size,
      :max_read_retries,
      :min_pool_size,
      :monitoring,
      :monitoring_io,
      :password,
      :platform,
      :read,
      :read_concern,
      :read_retry_interval,
      :replica_set,
      :retry_writes,
      :scan,
      :sdam_proc,
      :server_selection_timeout,
      :socket_timeout,
      :ssl,
      :ssl_ca_cert,
      :ssl_ca_cert_object,
      :ssl_ca_cert_string,
      :ssl_cert,
      :ssl_cert_object,
      :ssl_cert_string,
      :ssl_key,
      :ssl_key_object,
      :ssl_key_pass_phrase,
      :ssl_key_string,
      :ssl_verify,
      :ssl_verify_certificate,
      :ssl_verify_hostname,
      :truncate_logs,
      :user,
      :wait_queue_timeout,
      :write,
      :zlib_compression_level,
    ].freeze

    # The compression algorithms supported by the driver.
    #
    # @since 2.5.0
    VALID_COMPRESSORS = [ Mongo::Protocol::Compressed::ZLIB ].freeze

    # @return [ Mongo::Cluster ] cluster The cluster of servers for the client.
    attr_reader :cluster

    # @return [ Mongo::Database ] database The database the client is operating on.
    attr_reader :database

    # @return [ Hash ] options The configuration options.
    attr_reader :options

    # Delegate command and collections execution to the current database.
    def_delegators :@database, :command, :collections

    # Delegate subscription to monitoring.
    def_delegators :monitoring, :subscribe, :unsubscribe

    # @return [ Monitoring ] monitoring The monitoring.
    # @api private
    def monitoring
      if cluster
        cluster.monitoring
      else
        @monitoring
      end
    end
    private :monitoring

    # Determine if this client is equivalent to another object.
    #
    # @example Check client equality.
    #   client == other
    #
    # @param [ Object ] other The object to compare to.
    #
    # @return [ true, false ] If the objects are equal.
    #
    # @since 2.0.0
    def ==(other)
      return false unless other.is_a?(Client)
      cluster == other.cluster && options == other.options
    end
    alias_method :eql?, :==

    # Get a collection object for the provided collection name.
    #
    # @example Get the collection.
    #   client[:users]
    #
    # @param [ String, Symbol ] collection_name The name of the collection.
    # @param [ Hash ] options The options to the collection.
    #
    # @return [ Mongo::Collection ] The collection.
    #
    # @since 2.0.0
    def [](collection_name, options = {})
      database[collection_name, options]
    end

    # Get the hash value of the client.
    #
    # @example Get the client hash value.
    #   client.hash
    #
    # @return [ Integer ] The client hash value.
    #
    # @since 2.0.0
    def hash
      [cluster, options].hash
    end

    # Instantiate a new driver client.
    #
    # @example Instantiate a single server or mongos client.
    #   Mongo::Client.new(['127.0.0.1:27017'])
    #
    # @example Instantiate a client for a replica set.
    #   Mongo::Client.new(['127.0.0.1:27017', '127.0.0.1:27021'])
    #
    # @example Directly connect to a mongod in a replica set
    #   Mongo::Client.new(['127.0.0.1:27017'], :connect => :direct)
    #   # without `:connect => :direct`, Mongo::Client will discover and
    #   # connect to the replica set if given the address of a server in
    #   # a replica set
    #
    # @param [ Array<String>, String ] addresses_or_uri The array of server addresses in the
    #   form of host:port or a MongoDB URI connection string.
    # @param [ Hash ] options The options to be used by the client.
    #
    #
    # @option options [ String, Symbol ] :app_name Application name that is
    #   printed to the mongod logs upon establishing a connection in server
    #   versions >= 3.4.
    # @option options [ Symbol ] :auth_mech The authentication mechanism to
    #   use. One of :mongodb_cr, :mongodb_x509, :plain, :scram, :scram256
    # @option options [ Hash ] :auth_mech_properties
    # @option options [ String ] :auth_source The source to authenticate from.
    # @option options [ Array<String> ] :compressors A list of potential
    #   compressors to use, in order of preference. The driver chooses the
    #   first compressor that is also supported by the server. Currently the
    #   driver only supports 'zlib'.
    # @option options [ Symbol ] :connect The connection method to use. This
    #   forces the cluster to behave in the specified way instead of
    #   auto-discovering. One of :direct, :replica_set, :sharded
    # @option options [ Float ] :connect_timeout The timeout, in seconds, to
    #   attempt a connection.
    # @option options [ String ] :database The database to connect to.
    # @option options [ Float ] :heartbeat_frequency The number of seconds for
    #   the server monitor to refresh it's description via ismaster.
    # @option options [ Object ] :id_generator A custom object to generate ids
    #   for documents. Must respond to #generate.
    # @option options [ Integer ] :local_threshold The local threshold boundary
    #   in seconds for selecting a near server for an operation.
    # @option options [ Logger ] :logger A custom logger if desired.
    # @option options [ Integer ] :max_idle_time The maximum seconds a socket can remain idle
    #   since it has been checked in to the pool.
    # @option options [ Integer ] :max_pool_size The maximum size of the
    #   connection pool.
    # @option options [ Integer ] :max_read_retries The maximum number of read
    #   retries on mongos query failures.
    # @option options [ Integer ] :min_pool_size The minimum size of the
    #   connection pool.
    # @option options [ true, false ] :monitoring If false is given, the
    #   client is initialized without global SDAM event subscribers and
    #   will not publish SDAM events. Command monitoring and legacy events
    #   will still be published, and the driver will still perform SDAM and
    #   monitor its cluster in order to perform server selection. Built-in
    #   driver logging of SDAM events will be disabled because it is
    #   implemented through SDAM event subscription. Client#subscribe will
    #   succeed for all event types, but subscribers to SDAM events will
    #   not be invoked. Values other than false result in default behavior
    #   which is to perform normal SDAM event publication.
    # @option options [ true, false ] :monitoring_io For internal driver
    #   use only. Set to false to prevent SDAM-related I/O from being
    #   done by this client or servers under it. Note: setting this option
    #   to false will make the client non-functional. It is intended for
    #   use in tests which manually invoke SDAM state transitions.
    # @option options [ String ] :password The user's password.
    # @option options [ String ] :platform Platform information to include in
    #   the metadata printed to the mongod logs upon establishing a connection
    #   in server versions >= 3.4.
    # @option options [ Hash ] :read The read preference options. The hash
    #   may have the following items:
    #   - *:mode* -- read preference specified as a symbol; valid values are
    #     *:primary*, *:primary_preferred*, *:secondary*, *:secondary_preferred*
    #     and *:nearest*.
    #   - *:tag_sets* -- an array of hashes.
    #   - *:local_threshold*.
    # @option options [ Hash ] :read_concern The read concern option.
    # @option options [ Float ] :read_retry_interval The interval, in seconds,
    #   in which reads on a mongos are retried.
    # @option options [ Symbol ] :replica_set The name of the replica set to
    #   connect to. Servers not in this replica set will be ignored.
    # @option options [ true, false ] :retry_writes Retry writes once when
    #   connected to a replica set or sharded cluster versions 3.6 and up.
    # @option options [ true, false ] :scan Whether to scan all seeds
    #   in constructor. The default in driver version 2.x is to do so;
    #   driver version 3.x will not scan seeds in constructor. Opt in to the
    #   new behavior by setting this option to false. *Note:* setting
    #   this option to nil enables scanning seeds in constructor in driver
    #   version 2.x. Driver version 3.x will recognize this option but
    #   will ignore it and will never scan seeds in the constructor.
    # @option options [ Proc ] :sdam_proc A Proc to invoke with the client
    #   as the argument prior to performing server discovery and monitoring.
    #   Use this to set up SDAM event listeners to receive events dispatched
    #   during client construction.
    #
    #   Note: the client is not fully constructed when sdam_proc is invoked,
    #   in particular the cluster is nil at this time. sdam_proc should
    #   limit itself to calling #subscribe and #unsubscribe methods on the
    #   client only.
    # @option options [ Integer ] :server_selection_timeout The timeout in seconds
    #   for selecting a server for an operation.
    # @option options [ Float ] :socket_timeout The timeout, in seconds, to
    #   execute operations on a socket.
    # @option options [ true, false ] :ssl Whether to use SSL.
    # @option options [ String ] :ssl_ca_cert The file containing concatenated
    #   certificate authority certificates used to validate certs passed from the
    #   other end of the connection. One of :ssl_ca_cert, :ssl_ca_cert_string or
    #   :ssl_ca_cert_object (in order of priority) is required for :ssl_verify.
    # @option options [ Array<OpenSSL::X509::Certificate> ] :ssl_ca_cert_object An array of
    #   OpenSSL::X509::Certificate representing the certificate authority certificates used
    #   to validate certs passed from the other end of the connection. One of :ssl_ca_cert,
    #   :ssl_ca_cert_string or :ssl_ca_cert_object (in order of priority) is required for :ssl_verify.
    # @option options [ String ] :ssl_ca_cert_string A string containing concatenated
    #   certificate authority certificates used to validate certs passed from the
    #   other end of the connection. One of :ssl_ca_cert, :ssl_ca_cert_string or
    #   :ssl_ca_cert_object (in order of priority) is required for :ssl_verify.
    # @option options [ String ] :ssl_cert The certificate file used to identify
    #   the connection against MongoDB. This option, if present, takes precedence
    #   over the values of :ssl_cert_string and :ssl_cert_object
    # @option options [ OpenSSL::X509::Certificate ] :ssl_cert_object The OpenSSL::X509::Certificate
    #   used to identify the connection against MongoDB
    # @option options [ String ] :ssl_cert_string A string containing the PEM-encoded
    #   certificate used to identify the connection against MongoDB. This option, if present,
    #   takes precedence over the value of :ssl_cert_object
    # @option options [ String ] :ssl_key The private keyfile used to identify the
    #   connection against MongoDB. Note that even if the key is stored in the same
    #   file as the certificate, both need to be explicitly specified. This option,
    #   if present, takes precedence over the values of :ssl_key_string and :ssl_key_object
    # @option options [ OpenSSL::PKey ] :ssl_key_object The private key used to identify the
    #   connection against MongoDB
    # @option options [ String ] :ssl_key_pass_phrase A passphrase for the private key.
    # @option options [ String ] :ssl_key_string A string containing the PEM-encoded private key
    #   used to identify the connection against MongoDB. This parameter, if present,
    #   takes precedence over the value of option :ssl_key_object
    # @option options [ true, false ] :ssl_verify Whether to perform peer certificate validation and
    #   hostname verification. Note that the decision of whether to validate certificates will be
    #   overridden if :ssl_verify_certificate is set, and the decision of whether to validate
    #   hostnames will be overridden if :ssl_verify_hostname is set.
    # @option options [ true, false ] :ssl_verify_certificate Whether to perform peer certificate
    #   validation. This setting overrides :ssl_verify with respect to whether certificate
    #   validation is performed.
    # @option options [ true, false ] :ssl_verify_hostname Whether to perform peer hostname
    #   validation. This setting overrides :ssl_verify with respect to whether hostname validation
    #   is performed.
    # @option options [ true, false ] :truncate_logs Whether to truncate the
    #   logs at the default 250 characters.
    # @option options [ String ] :user The user name.
    # @option options [ Float ] :wait_queue_timeout The time to wait, in
    #   seconds, in the connection pool for a connection to be checked in.
    # @option options [ Hash ] :write The write concern options. Can be :w =>
    #   Integer|String, :fsync => Boolean, :j => Boolean.
    # @option options [ Integer ] :zlib_compression_level The Zlib compression level to use, if using compression.
    #   See Ruby's Zlib module for valid levels.
    #
    # @since 2.0.0
    def initialize(addresses_or_uri, options = nil)
      if options
        options = options.dup
      else
        options = {}
      end
      Lint.validate_underscore_read_preference(options[:read])
      Lint.validate_read_concern_option(options[:read_concern])
      if addresses_or_uri.is_a?(::String)
        uri = URI.get(addresses_or_uri, options)
        addresses = uri.servers
        options = uri.client_options.merge(options)
      else
        addresses = addresses_or_uri
      end
      # Special handling for sdam_proc as it is only used during client
      # construction
      sdam_proc = options.delete(:sdam_proc)
      @options = validate_options!(Database::DEFAULT_OPTIONS.merge(options)).freeze
      @database = Database.new(self, @options[:database], @options)
      # Temporarily set monitoring so that event subscriptions can be
      # set up without there being a cluster
      @monitoring = Monitoring.new(@options)
      if sdam_proc
        sdam_proc.call(self)
      end
      @server_selection_semaphore = Semaphore.new
      @cluster = Cluster.new(addresses, @monitoring, cluster_options)
      # Unset monitoring, it will be taken out of cluster from now on
      remove_instance_variable('@monitoring')

      yield(self) if block_given?
    end

    # @api private
    def cluster_options
      # We share clusters when a new client with different CRUD_OPTIONS
      # is requested; therefore, cluster should not be getting any of these
      # options upon instantiation
      options.reject do |key, value|
        CRUD_OPTIONS.include?(key.to_sym)
      end.merge(
        server_selection_semaphore: @server_selection_semaphore,
        # but need to put the database back in for auth...
        database: options[:database],
      )
    end

    # Get an inspection of the client as a string.
    #
    # @example Inspect the client.
    #   client.inspect
    #
    # @return [ String ] The inspection string.
    #
    # @since 2.0.0
    def inspect
      "#<Mongo::Client:0x#{object_id} cluster=#{cluster.summary}>"
    end

    # Get a summary of the client state.
    #
    # @note This method is experimental and subject to change.
    #
    # @example Inspect the client.
    #   client.summary
    #
    # @return [ String ] Summary string.
    #
    # @since 2.7.0
    # @api experimental
    def summary
      "#<Client cluster=#{cluster.summary}>"
    end

    # Get the server selector. It either uses the read preference
    # defined in the client options or defaults to a Primary server selector.
    #
    # @example Get the server selector.
    #   client.server_selector
    #
    # @return [ Mongo::ServerSelector ] The server selector using the
    #  user-defined read preference or a Primary server selector default.
    #
    # @since 2.5.0
    def server_selector
      @server_selector ||= ServerSelector.get(read_preference || ServerSelector::PRIMARY)
    end

    # Get the read preference from the options passed to the client.
    #
    # @example Get the read preference.
    #   client.read_preference
    #
    # @return [ BSON::Document ] The user-defined read preference.
    #   The document may have the following fields:
    #   - *:read* -- read preference specified as a symbol; valid values are
    #     *:primary*, *:primary_preferred*, *:secondary*, *:secondary_preferred*
    #     and *:nearest*.
    #   - *:tag_sets* -- an array of hashes.
    #   - *:local_threshold*.
    #
    # @since 2.0.0
    def read_preference
      @read_preference ||= options[:read]
    end

    # Creates a new client configured to use the database with the provided
    # name, and using the other options configured in this client.
    #
    # @note The new client shares the cluster with the original client,
    #   and as a result also shares the monitoring instance and monitoring
    #   event subscribers.
    #
    # @example Create a client for the `users' database.
    #   client.use(:users)
    #
    # @param [ String, Symbol ] name The name of the database to use.
    #
    # @return [ Mongo::Client ] A new client instance.
    #
    # @since 2.0.0
    def use(name)
      with(database: name)
    end

    # Creates a new client with the passed options merged over the existing
    # options of this client. Useful for one-offs to change specific options
    # without altering the original client.
    #
    # @note Depending on options given, the returned client may share the
    #   cluster with the original client or be created with a new cluster.
    #   If a new cluster is created, the monitoring event subscribers on
    #   the new client are set to the default event subscriber set and
    #   none of the subscribers on the original client are copied over.
    #
    # @example Get a client with changed options.
    #   client.with(:read => { :mode => :primary_preferred })
    #
    # @param [ Hash ] new_options The new options to use.
    #
    # @return [ Mongo::Client ] A new client instance.
    #
    # @since 2.0.0
    def with(new_options = Options::Redacted.new)
      clone.tap do |client|
        opts = validate_options!(new_options)
        client.options.update(opts)
        Database.create(client)
        # We can't use the same cluster if some options that would affect it
        # have changed.
        if cluster_modifying?(opts)
          Cluster.create(client)
        end
      end
    end

    # Get the read concern for this client.
    #
    # @example Get the client read concern.
    #   client.read_concern
    #
    # @return [ Hash ] The read concern.
    #
    # @since 2.6.0
    def read_concern
      options[:read_concern]
    end


    # Get the write concern for this client. If no option was provided, then a
    # default single server acknowledgement will be used.
    #
    # @example Get the client write concern.
    #   client.write_concern
    #
    # @return [ Mongo::WriteConcern ] The write concern.
    #
    # @since 2.0.0
    def write_concern
      @write_concern ||= WriteConcern.get(options[:write])
    end

    # Close all connections.
    #
    # @example Disconnect the client.
    #   client.close
    #
    # @param [ Boolean ] wait Whether to wait for background threads to
    #   finish running.
    #
    # @return [ true ] Always true.
    #
    # @since 2.1.0
    def close(wait=false)
      @cluster.disconnect!(wait)
      true
    end

    # Reconnect the client.
    #
    # @example Reconnect the client.
    #   client.reconnect
    #
    # @return [ true ] Always true.
    #
    # @since 2.1.0
    def reconnect
      addresses = cluster.addresses.map(&:to_s)

      @cluster.disconnect! rescue nil

      @cluster = Cluster.new(addresses, monitoring, cluster_options)
      true
    end

    # Get the names of all databases.
    #
    # @example Get the database names.
    #   client.database_names
    #
    # @param [ Hash ] filter The filter criteria for getting a list of databases.
    # @param [ Hash ] opts The command options.
    #
    # @return [ Array<String> ] The names of the databases.
    #
    # @since 2.0.5
    def database_names(filter = {}, opts = {})
      list_databases(filter, true, opts).collect{ |info| info[Database::NAME] }
    end

    # Get info for each database.
    #
    # @example Get the info for each database.
    #   client.list_databases
    #
    # @param [ Hash ] filter The filter criteria for getting a list of databases.
    # @param [ true, false ] name_only Whether to only return each database name without full metadata.
    # @param [ Hash ] opts The command options.
    #
    # @return [ Array<Hash> ] The info for each database.
    #
    # @since 2.0.5
    def list_databases(filter = {}, name_only = false, opts = {})
      cmd = { listDatabases: 1 }
      cmd[:nameOnly] = !!name_only
      cmd[:filter] = filter unless filter.empty?
      use(Database::ADMIN).command(cmd, opts).first[Database::DATABASES]
    end

    # Returns a list of Mongo::Database objects.
    #
    # @example Get a list of Mongo::Database objects.
    #   client.list_mongo_databases
    #
    # @param [ Hash ] filter The filter criteria for getting a list of databases.
    # @param [ Hash ] opts The command options.
    #
    # @return [ Array<Mongo::Database> ] The list of database objects.
    #
    # @since 2.5.0
    def list_mongo_databases(filter = {}, opts = {})
      database_names(filter, opts).collect do |name|
        Database.new(self, name, options)
      end
    end

    # Start a session.
    #
    # If the deployment does not support sessions, raises
    # Mongo::Error::InvalidSession. This exception can also be raised when
    # the driver is not connected to a data-bearing server, for example
    # during failover.
    #
    # @example Start a session.
    #   client.start_session(causal_consistency: true)
    #
    # @param [ Hash ] options The session options. Accepts the options
    #   that Session#initialize accepts.
    #
    # @note A Session cannot be used by multiple threads at once; session
    #   objects are not thread-safe.
    #
    # @return [ Session ] The session.
    #
    # @since 2.5.0
    def start_session(options = {})
      cluster.send(:get_session, self, options.merge(implicit: false)) ||
        (raise Error::InvalidSession.new(Session::SESSIONS_NOT_SUPPORTED))
    end

    # As of version 3.6 of the MongoDB server, a ``$changeStream`` pipeline stage is supported
    # in the aggregation framework. As of version 4.0, this stage allows users to request that
    # notifications are sent for all changes that occur in the client's cluster.
    #
    # @example Get change notifications for the client's cluster.
    #  client.watch([{ '$match' => { operationType: { '$in' => ['insert', 'replace'] } } }])
    #
    # @param [ Array<Hash> ] pipeline Optional additional filter operators.
    # @param [ Hash ] options The change stream options.
    #
    # @option options [ String ] :full_document Allowed values: 'default', 'updateLookup'.
    #   Defaults to 'default'. When set to 'updateLookup', the change notification for partial
    #   updates will include both a delta describing the changes to the document, as well as a copy
    #   of the entire document that was changed from some time after the change occurred.
    # @option options [ BSON::Document, Hash ] :resume_after Specifies the logical starting point
    #   for the new change stream.
    # @option options [ Integer ] :max_await_time_ms The maximum amount of time for the server to
    #   wait on new documents to satisfy a change stream query.
    # @option options [ Integer ] :batch_size The number of documents to return per batch.
    # @option options [ BSON::Document, Hash ] :collation The collation to use.
    # @option options [ Session ] :session The session to use.
    # @option options [ BSON::Timestamp ] :start_at_operation_time Only return
    #   changes that occurred at or after the specified timestamp. Any command run
    #   against the server will return a cluster time that can be used here.
    #   Only recognized by server versions 4.0+.
    #
    # @note A change stream only allows 'majority' read concern.
    # @note This helper method is preferable to running a raw aggregation with a $changeStream
    #   stage, for the purpose of supporting resumability.
    #
    # @return [ ChangeStream ] The change stream object.
    #
    # @since 2.6.0
    def watch(pipeline = [], options = {})
      return use(Database::ADMIN).watch(pipeline, options) unless database.name == Database::ADMIN

      Mongo::Collection::View::ChangeStream.new(
        Mongo::Collection::View.new(self["#{Database::COMMAND}.aggregate"]),
        pipeline,
        Mongo::Collection::View::ChangeStream::CLUSTER,
        options)
    end

    private

    # If options[:session] is set, validates that session and returns it.
    # If deployment supports sessions, creates a new session and returns it.
    # The session is implicit unless options[:implicit] is given.
    # If deployment does not support session, returns nil.
    #
    # @note This method will return nil if deployment has no data-bearing
    #   servers at the time of the call.
    def get_session(options = {})
      cluster.send(:get_session, self, options)
    end

    def with_session(options = {}, &block)
      cluster.send(:with_session, self, options, &block)
    end

    def initialize_copy(original)
      @options = original.options.dup
      @monitoring = @cluster ? monitoring : Monitoring.new(options)
      @database = nil
      @read_preference = nil
      @write_concern = nil
    end

    def cluster_modifying?(new_options)
      cluster_options = new_options.reject do |name|
        CRUD_OPTIONS.include?(name.to_sym)
      end
      cluster_options.any? do |name, value|
        options[name] != value
      end
    end

    def validate_options!(opts = Options::Redacted.new)
      return Options::Redacted.new unless opts
      opts.each.inject(Options::Redacted.new) do |_options, (k, v)|
        key = k.to_sym
        if VALID_OPTIONS.include?(key)
          validate_max_min_pool_size!(key, opts)
          validate_read!(key, opts)
          if key == :compressors
            compressors = valid_compressors(v)
            _options[key] = compressors unless compressors.empty?
          else
            _options[key] = v
          end
        else
          log_warn("Unsupported client option '#{k}'. It will be ignored.")
        end
        _options
      end
    end

    def valid_compressors(compressors)
      compressors.select do |compressor|
        if !VALID_COMPRESSORS.include?(compressor)
          log_warn("Unsupported compressor '#{compressor}' in list '#{compressors}'. " +
                       "This compressor will not be used.")
          false
        else
          true
        end
      end
    end

    def validate_max_min_pool_size!(option, opts)
      if option == :min_pool_size && opts[:min_pool_size]
        max = opts[:max_pool_size] || Server::ConnectionPool::AvailableStack::MAX_SIZE
        raise Error::InvalidMinPoolSize.new(opts[:min_pool_size], max) unless opts[:min_pool_size] <= max
      end
      true
    end

    def validate_read!(option, opts)
      if option == :read && opts.has_key?(:read)
        read = opts[:read]
        # We could check if read is a Hash, but this would fail
        # for custom classes implementing key access ([]).
        # Instead reject common cases of strings and symbols.
        if read.is_a?(String) || read.is_a?(Symbol)
          raise Error::InvalidReadOption.new(read, 'must be a hash')
        end

        if mode = read[:mode]
          mode = mode.to_sym
          unless Mongo::ServerSelector::PREFERENCES.include?(mode)
            raise Error::InvalidReadOption.new(read, "mode #{mode} is not one of recognized modes")
          end
        end
      end
      true
    end
  end
end

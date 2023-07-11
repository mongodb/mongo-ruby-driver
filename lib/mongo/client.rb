# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2014-2020 MongoDB Inc.
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
    CRUD_OPTIONS = [
      :auto_encryption_options,
      :database,
      :read, :read_concern,
      :write, :write_concern,
      :retry_reads, :max_read_retries, :read_retry_interval,
      :retry_writes, :max_write_retries,

      # Options which cannot currently be here:
      #
      # :server_selection_timeout
      # Server selection timeout is used by cluster constructor to figure out
      # how long to wait for initial scan in compatibility mode, but once
      # the cluster is initialized it no longer uses this timeout.
      # Unfortunately server selector reads server selection timeout out of
      # the cluster, and this behavior is required by Cluster#next_primary
      # which takes no arguments. When next_primary is removed we can revsit
      # using the same cluster object with different server selection timeouts.
    ].freeze

    # Valid client options.
    #
    # @since 2.1.2
    VALID_OPTIONS = [
      :app_name,
      :auth_mech,
      :auth_mech_properties,
      :auth_source,
      :auto_encryption_options,
      :bg_error_backtrace,
      :cleanup,
      :compressors,
      :direct_connection,
      :connect,
      :connect_timeout,
      :database,
      :heartbeat_frequency,
      :id_generator,
      :load_balanced,
      :local_threshold,
      :logger,
      :log_prefix,
      :max_connecting,
      :max_idle_time,
      :max_pool_size,
      :max_read_retries,
      :max_write_retries,
      :min_pool_size,
      :monitoring,
      :monitoring_io,
      :password,
      :platform,
      :populator_io,
      :read,
      :read_concern,
      :read_retry_interval,
      :replica_set,
      :resolv_options,
      :retry_reads,
      :retry_writes,
      :scan,
      :sdam_proc,
      :server_api,
      :server_selection_timeout,
      :socket_timeout,
      :srv_max_hosts,
      :srv_service_name,
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
      :ssl_verify_ocsp_endpoint,
      :truncate_logs,
      :user,
      :wait_queue_timeout,
      :wrapping_libraries,
      :write,
      :write_concern,
      :zlib_compression_level,
    ].freeze

    # The compression algorithms supported by the driver.
    #
    # @since 2.5.0
    VALID_COMPRESSORS = [
      Mongo::Protocol::Compressed::ZSTD,
      Mongo::Protocol::Compressed::SNAPPY,
      Mongo::Protocol::Compressed::ZLIB
    ].freeze

    # The known server API versions.
    VALID_SERVER_API_VERSIONS = %w(
      1
    ).freeze

    # @return [ Mongo::Cluster ] cluster The cluster of servers for the client.
    attr_reader :cluster

    # @return [ Mongo::Database ] database The database the client is operating on.
    attr_reader :database

    # @return [ Hash ] options The configuration options.
    attr_reader :options

    # @return [ Mongo::Crypt::AutoEncrypter ] The object that encapsulates
    #   auto-encryption behavior
    attr_reader :encrypter

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
    # @param [ Array<String> | String ] addresses_or_uri The array of server addresses in the
    #   form of host:port or a MongoDB URI connection string.
    # @param [ Hash ] options The options to be used by the client. If a MongoDB URI
    #   connection string is also provided, these options take precedence over any
    #   analogous options present in the URI string.
    #
    # @option options [ String, Symbol ] :app_name Application name that is
    #   printed to the mongod logs upon establishing a connection in server
    #   versions >= 3.4.
    # @option options [ Symbol ] :auth_mech The authentication mechanism to
    #   use. One of :mongodb_cr, :mongodb_x509, :plain, :scram, :scram256
    # @option options [ Hash ] :auth_mech_properties
    # @option options [ String ] :auth_source The source to authenticate from.
    # @option options [ true | false | nil | Integer ] :bg_error_backtrace
    #   Experimental. Set to true to log complete backtraces for errors in
    #   background threads. Set to false or nil to not log backtraces. Provide
    #   a positive integer to log up to that many backtrace lines.
    # @option options [ Array<String> ] :compressors A list of potential
    #   compressors to use, in order of preference. The driver chooses the
    #   first compressor that is also supported by the server. Currently the
    #   driver only supports 'zstd, 'snappy' and 'zlib'.
    # @option options [ true | false ] :direct_connection Whether to connect
    #   directly to the specified seed, bypassing topology discovery. Exactly
    #   one seed must be provided.
    # @option options [ Symbol ] :connect Deprecated - use :direct_connection
    #   option instead of this option. The connection method to use. This
    #   forces the cluster to behave in the specified way instead of
    #   auto-discovering. One of :direct, :replica_set, :sharded,
    #   :load_balanced. If :connect is set to :load_balanced, the driver
    #   will behave as if the server is a load balancer even if it isn't
    #   connected to a load balancer.
    # @option options [ Float ] :connect_timeout The timeout, in seconds, to
    #   attempt a connection.
    # @option options [ String ] :database The database to connect to.
    # @option options [ Float ] :heartbeat_frequency The interval, in seconds,
    #   for the server monitor to refresh its description via hello.
    # @option options [ Object ] :id_generator A custom object to generate ids
    #   for documents. Must respond to #generate.
    # @option options [ true | false ] :load_balanced Whether to expect to
    #   connect to a load balancer.
    # @option options [ Integer ] :local_threshold The local threshold boundary
    #   in seconds for selecting a near server for an operation.
    # @option options [ Logger ] :logger A custom logger to use.
    # @option options [ String ] :log_prefix A custom log prefix to use when
    #   logging. This option is experimental and subject to change in a future
    #   version of the driver.
    # @option options [ Integer ] :max_connecting The maximum number of
    #  connections that can be connecting simultaneously. The default is 2.
    #  This option should be increased if there are many threads that share
    #  the same client and the application is experiencing timeouts
    #  while waiting for connections to be established.
    #  selecting a server for an operation. The default is 2.
    # @option options [ Integer ] :max_idle_time The maximum seconds a socket can remain idle
    #   since it has been checked in to the pool.
    # @option options [ Integer ] :max_pool_size The maximum size of the
    #   connection pool. Setting this option to zero creates an unlimited connection pool.
    # @option options [ Integer ] :max_read_retries The maximum number of read
    #   retries when legacy read retries are in use.
    # @option options [ Integer ] :max_write_retries The maximum number of write
    #   retries when legacy write retries are in use.
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
    # @option options [ true | false ] :cleanup For internal driver use only.
    #   Set to false to prevent endSessions command being sent to the server
    #   to clean up server sessions when the cluster is disconnected, and to
    #   to not start the periodic executor. If :monitoring_io is false,
    #   :cleanup automatically defaults to false as well.
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
    # @option options [ true | false ] :retry_reads If true, modern retryable
    #   reads are enabled (which is the default). If false, modern retryable
    #   reads are disabled and legacy retryable reads are enabled.
    # @option options [ true | false ] :retry_writes Retry writes once when
    #   connected to a replica set or sharded cluster versions 3.6 and up.
    #   (Default is true.)
    # @option options [ true | false ] :scan Whether to scan all seeds
    #   in constructor. The default in driver version 2.x is to do so;
    #   driver version 3.x will not scan seeds in constructor. Opt in to the
    #   new behavior by setting this option to false. *Note:* setting
    #   this option to nil enables scanning seeds in constructor in driver
    #   version 2.x. Driver version 3.x will recognize this option but
    #   will ignore it and will never scan seeds in the constructor.
    # @option options [ Proc ] :sdam_proc A Proc to invoke with the client
    #   as the argument prior to performing server discovery and monitoring.
    #   Use this to set up SDAM event listeners to receive events published
    #   during client construction.
    #
    #   Note: the client is not fully constructed when sdam_proc is invoked,
    #   in particular the cluster is nil at this time. sdam_proc should
    #   limit itself to calling #subscribe and #unsubscribe methods on the
    #   client only.
    # @option options [ Hash ] :server_api The requested server API version.
    #   This hash can have the following items:
    #   - *:version* -- string
    #   - *:strict* -- boolean
    #   - *:deprecation_errors* -- boolean
    # @option options [ Integer ] :server_selection_timeout The timeout in seconds
    #   for selecting a server for an operation.
    # @option options [ Float ] :socket_timeout The timeout, in seconds, to
    #   execute operations on a socket.
    # @option options [ Integer ] :srv_max_hosts The maximum number of mongoses
    #   that the driver will communicate with for sharded topologies. If this
    #   option is 0, then there will be no maximum number of mongoses. If the
    #   given URI resolves to more hosts than ``:srv_max_hosts``, the client
    #   will ramdomly choose an ``:srv_max_hosts`` sized subset of hosts.
    # @option options [ String ] :srv_service_name The service name to use in
    #   the SRV DNS query.
    # @option options [ true, false ] :ssl Whether to use TLS.
    # @option options [ String ] :ssl_ca_cert The file containing concatenated
    #   certificate authority certificates used to validate certs passed from the
    #   other end of the connection. Intermediate certificates should NOT be
    #   specified in files referenced by this option. One of :ssl_ca_cert,
    #   :ssl_ca_cert_string or :ssl_ca_cert_object (in order of priority) is
    #   required when using :ssl_verify.
    # @option options [ Array<OpenSSL::X509::Certificate> ] :ssl_ca_cert_object
    #   An array of OpenSSL::X509::Certificate objects representing the
    #   certificate authority certificates used to validate certs passed from
    #   the other end of the connection. Intermediate certificates should NOT
    #   be specified in files referenced by this option. One of :ssl_ca_cert,
    #   :ssl_ca_cert_string or :ssl_ca_cert_object (in order of priority)
    #   is required when using :ssl_verify.
    # @option options [ String ] :ssl_ca_cert_string A string containing
    #   certificate authority certificate used to validate certs passed from the
    #   other end of the connection. This option allows passing only one CA
    #   certificate to the driver. Intermediate certificates should NOT
    #   be specified in files referenced by this option. One of :ssl_ca_cert,
    #   :ssl_ca_cert_string or :ssl_ca_cert_object (in order of priority) is
    #   required when using :ssl_verify.
    # @option options [ String ] :ssl_cert The certificate file used to identify
    #   the connection against MongoDB. A certificate chain may be passed by
    #   specifying the client certificate first followed by any intermediate
    #   certificates up to the CA certificate. The file may also contain the
    #   certificate's private key, which will be ignored. This option, if present,
    #   takes precedence over the values of :ssl_cert_string and :ssl_cert_object
    # @option options [ OpenSSL::X509::Certificate ] :ssl_cert_object The OpenSSL::X509::Certificate
    #   used to identify the connection against MongoDB. Only one certificate
    #   may be passed through this option.
    # @option options [ String ] :ssl_cert_string A string containing the PEM-encoded
    #   certificate used to identify the connection against MongoDB. A certificate
    #   chain may be passed by specifying the client certificate first followed
    #   by any intermediate certificates up to the CA certificate. The string
    #   may also contain the certificate's private key, which will be ignored,
    #   This option, if present, takes precedence over the value of :ssl_cert_object
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
    # @option options [ Array<Hash> ] :wrapping_libraries Information about
    #   libraries such as ODMs that are wrapping the driver, to be added to
    #    metadata sent to the server. Specify the lower level libraries first.
    #    Allowed hash keys: :name, :version, :platform.
    # @option options [ Hash ] :write Deprecated. Equivalent to :write_concern
    #   option.
    # @option options [ Hash ] :write_concern The write concern options.
    #   Can be :w => Integer|String, :wtimeout => Integer (in milliseconds),
    #   :j => Boolean, :fsync => Boolean.
    # @option options [ Integer ] :zlib_compression_level The Zlib compression level to use, if using compression.
    #   See Ruby's Zlib module for valid levels.
    # @option options [ Hash ] :resolv_options For internal driver use only.
    #   Options to pass through to Resolv::DNS constructor for SRV lookups.
    # @option options [ Hash ] :auto_encryption_options Auto-encryption related
    #   options.
    #   - :key_vault_client => Client | nil, a client connected to the MongoDB
    #     instance containing the encryption key vault
    #   - :key_vault_namespace => String, the namespace of the key vault in the
    #     format database.collection
    #   - :kms_providers => Hash, A hash of key management service (KMS) configuration
    #     information. Valid hash keys are :aws, :azure, :gcp, :kmip, :local.
    #     There may be more than one kms provider specified.
    #   - :kms_tls_options => Hash, A hash of TLS options to authenticate to
    #     KMS providers, usually used for KMIP servers. Valid hash keys
    #     are :aws, :azure, :gcp, :kmip, :local. There may be more than one
    #     kms provider specified.
    #   - :schema_map => Hash | nil, JSONSchema for one or more collections
    #     specifying which fields should be encrypted. This option is
    #     mutually exclusive with :schema_map_path.
    #     - Note: Schemas supplied in the schema_map only apply to configuring
    #       automatic encryption for client side encryption. Other validation
    #       rules in the JSON schema will not be enforced by the driver and will
    #       result in an error.
    #     - Note: Supplying a schema_map provides more security than relying on
    #       JSON Schemas obtained from the server. It protects against a
    #       malicious server advertising a false JSON Schema, which could trick
    #       the client into sending unencrypted data that should be encrypted.
    #     - Note: If a collection is present on both the :encrypted_fields_map
    #       and :schema_map, an error will be raised.
    #   - :schema_map_path => String | nil A path to a file contains the JSON schema
    #   of the collection that stores auto encrypted documents. This option is
    #   mutually exclusive with :schema_map.
    #   - :bypass_auto_encryption => Boolean, when true, disables auto encryption;
    #     defaults to false.
    #   - :extra_options => Hash | nil, options related to spawning mongocryptd
    #     (this part of the API is subject to change).
    #   - :encrypted_fields_map => Hash | nil, maps a collection namespace to
    #     a hash describing encrypted fields for queryable encryption.
    #     - Note: If a collection is present on both the encryptedFieldsMap
    #       and schemaMap, an error will be raised.
    #   - :bypass_query_analysis => Boolean | nil, when true disables automatic
    #     analysis of outgoing commands.
    #   - :crypt_shared_lib_path => [ String | nil ]  Path that should
    #     be  the used to load the crypt shared library. Providing this option
    #     overrides default crypt shared library load paths for libmongocrypt.
    #   - :crypt_shared_lib_required => [ Boolean | nil ]  Whether
    #     crypt shared library is required. If 'true', an error will be raised
    #     if a crypt_shared library cannot be loaded by libmongocrypt.
    #
    #   Notes on automatic encryption:
    #   - Automatic encryption is an enterprise only feature that only applies
    #     to operations on a collection.
    #   - Automatic encryption is not supported for operations on a database or
    #     view.
    #   - Automatic encryption requires the authenticated user to have the
    #     listCollections privilege.
    #   - At worst, automatic encryption may triple the number of connections
    #     used by the Client at any one time.
    #   - If automatic encryption fails on an operation, use a MongoClient
    #     configured with bypass_auto_encryption: true and use
    #     ClientEncryption.encrypt to manually encrypt values.
    #   - Enabling Client Side Encryption reduces the maximum write batch size
    #     and may have a negative performance impact.
    #
    # @since 2.0.0
    def initialize(addresses_or_uri, options = nil)
      options = options ? options.dup : {}

      srv_uri = nil
      if addresses_or_uri.is_a?(::String)
        uri = URI.get(addresses_or_uri, options)
        if uri.is_a?(URI::SRVProtocol)
          # If the URI is an SRV URI, note this so that we can start
          # SRV polling if the topology is a sharded cluster.
          srv_uri = uri
        end
        addresses = uri.servers
        uri_options = uri.client_options.dup
        # Special handing for :write and :write_concern: allow client Ruby
        # options to override URI options, even when the Ruby option uses the
        # deprecated :write key and the URI option uses the current
        # :write_concern key
        if options[:write]
          uri_options.delete(:write_concern)
        end
        options = uri_options.merge(options)
        @srv_records = uri.srv_records
      else
        addresses = addresses_or_uri
        addresses.each do |addr|
          if addr =~ /\Amongodb(\+srv)?:\/\//i
            raise ArgumentError, "Host '#{addr}' should not contain protocol. Did you mean to not use an array?"
          end
        end

        @srv_records = nil
      end

      options = self.class.canonicalize_ruby_options(options)

      # The server API version is specified to be a string.
      # However, it is very annoying to always provide the number 1 as a string,
      # therefore cast to the string type here.
      if server_api = options[:server_api]
        if server_api.is_a?(Hash)
          server_api = Options::Redacted.new(server_api)
          if (version = server_api[:version]).is_a?(Integer)
            options[:server_api] = server_api.merge(version: version.to_s)
          end
        end
      end

      # Special handling for sdam_proc as it is only used during client
      # construction
      sdam_proc = options.delete(:sdam_proc)

      # For gssapi service_name, the default option is given in a hash
      # (one level down from the top level).
      merged_options = default_options(options)
      options.each do |k, v|
        default_v = merged_options[k]
        if Hash === default_v
          v = default_v.merge(v)
        end
        merged_options[k] = v
      end
      options = merged_options

      options.keys.each do |k|
        if options[k].nil?
          options.delete(k)
        end
      end

      @options = validate_new_options!(options)
=begin WriteConcern object support
      if @options[:write_concern].is_a?(WriteConcern::Base)
        # Cache the instance so that we do not needlessly reconstruct it.
        @write_concern = @options[:write_concern]
        @options[:write_concern] = @write_concern.options
      end
=end
      @options.freeze
      validate_options!(addresses, is_srv: uri.is_a?(URI::SRVProtocol))
      validate_authentication_options!

      database_options = @options.dup
      database_options.delete(:server_api)
      @database = Database.new(self, @options[:database], database_options)

      # Temporarily set monitoring so that event subscriptions can be
      # set up without there being a cluster
      @monitoring = Monitoring.new(@options)

      if sdam_proc
        sdam_proc.call(self)
      end

      @connect_lock = Mutex.new
      @connect_lock.synchronize do
        @cluster = Cluster.new(addresses, @monitoring,
          cluster_options.merge(srv_uri: srv_uri))
      end

      begin
        # Unset monitoring, it will be taken out of cluster from now on
        remove_instance_variable('@monitoring')

        if @options[:auto_encryption_options]
          @connect_lock.synchronize do
            build_encrypter
          end
        end

      rescue
        begin
          @cluster.close
        rescue => e
          log_warn("Eror closing cluster in client constructor's exception handler: #{e.class}: #{e}")
          # Drop this exception so that the original exception is raised
        end
        raise
      end

      if block_given?
        begin
          yield(self)
        ensure
          close
        end
      end
    end

    # @api private
    def cluster_options
      # We share clusters when a new client with different CRUD_OPTIONS
      # is requested; therefore, cluster should not be getting any of these
      # options upon instantiation
      options.reject do |key, value|
        CRUD_OPTIONS.include?(key.to_sym)
      end.merge(
        # but need to put the database back in for auth...
        database: options[:database],

        # Put these options in for legacy compatibility, but note that
        # their values on the client and the cluster do not have to match -
        # applications should read these values from client, not from cluster
        max_read_retries: options[:max_read_retries],
        read_retry_interval: options[:read_retry_interval],
      ).tap do |options|
        # If the client has a cluster already, forward srv_uri to the new
        # cluster to maintain SRV monitoring. If the client is brand new,
        # its constructor sets srv_uri manually.
        if cluster
          options.update(srv_uri: cluster.options[:srv_uri])
        end
      end
    end

    # Get the maximum number of times the client can retry a read operation
    # when using legacy read retries.
    #
    # @return [ Integer ] The maximum number of retries.
    #
    # @api private
    def max_read_retries
      options[:max_read_retries] || Cluster::MAX_READ_RETRIES
    end

    # Get the interval, in seconds, in which read retries when using legacy
    # read retries.
    #
    # @return [ Float ] The interval.
    #
    # @api private
    def read_retry_interval
      options[:read_retry_interval] || Cluster::READ_RETRY_INTERVAL
    end

    # Get the maximum number of times the client can retry a write operation
    # when using legacy write retries.
    #
    # @return [ Integer ] The maximum number of retries.
    #
    # @api private
    def max_write_retries
      options[:max_write_retries] || Cluster::MAX_WRITE_RETRIES
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
    # @note The exact format and layout of the returned summary string is
    #   not part of the driver's public API and may be changed at any time.
    #
    # @return [ String ] The summary string.
    #
    # @since 2.7.0
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
      @server_selector ||= if read_preference
        ServerSelector.get(read_preference)
      else
        ServerSelector.primary
      end
    end

    # Get the read preference from the options passed to the client.
    #
    # @example Get the read preference.
    #   client.read_preference
    #
    # @return [ BSON::Document ] The user-defined read preference.
    #   The document may have the following fields:
    #   - *:mode* -- read preference specified as a symbol; valid values are
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
    def with(new_options = nil)
      clone.tap do |client|
        opts = client.update_options(new_options || Options::Redacted.new)
        Database.create(client)
        # We can't use the same cluster if some options that would affect it
        # have changed.
        if cluster_modifying?(opts)
          Cluster.create(client, monitoring: opts[:monitoring])
        end
      end
    end

    # Updates this client's options from new_options, validating all options.
    #
    # The new options may be transformed according to various rules.
    # The final hash of options actually applied to the client is returned.
    #
    # If options fail validation, this method may warn or raise an exception.
    # If this method raises an exception, the client should be discarded
    # (similarly to if a constructor raised an exception).
    #
    # @param [ Hash ] new_options The new options to use.
    #
    # @return [ Hash ] Modified new options written into the client.
    #
    # @api private
    def update_options(new_options)
      old_options = @options

      new_options = self.class.canonicalize_ruby_options(new_options || {})

      validate_new_options!(new_options).tap do |opts|
        # Our options are frozen
        options = @options.dup
        if options[:write] && opts[:write_concern]
          options.delete(:write)
        end
        if options[:write_concern] && opts[:write]
          options.delete(:write_concern)
        end

        options.update(opts)
        @options = options.freeze

        auto_encryption_options_changed =
          @options[:auto_encryption_options] != old_options[:auto_encryption_options]

        # If there are new auto_encryption_options, create a new encrypter.
        # Otherwise, allow the new client to share an encrypter with the
        # original client.
        #
        # If auto_encryption_options are nil, set @encrypter to nil, but do not
        # close the encrypter because it may still be used by the original client.
        if @options[:auto_encryption_options] && auto_encryption_options_changed
          @connect_lock.synchronize do
            build_encrypter
          end
        elsif @options[:auto_encryption_options].nil?
          @connect_lock.synchronize do
            @encrypter = nil
          end
        end

        validate_options!
        validate_authentication_options!
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
      @write_concern ||= WriteConcern.get(options[:write_concern] || options[:write])
    end

    def closed?
      !!@closed
    end

    # Close all connections.
    #
    # @return [ true ] Always true.
    #
    # @since 2.1.0
    def close
      @connect_lock.synchronize do
        @closed = true
        do_close
      end
      true
    end

    # Close encrypter and clean up auto-encryption resources.
    #
    # @return [ true ] Always true.
    def close_encrypter
      @encrypter.close if @encrypter

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

      @connect_lock.synchronize do
        do_close rescue nil

        @cluster = Cluster.new(addresses, monitoring, cluster_options)

        if @options[:auto_encryption_options]
          build_encrypter
        end

        @closed = false
      end

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
    # @option opts [ true, false ] :authorized_databases A flag that determines
    #   which databases are returned based on user privileges when access control
    #   is enabled
    #
    #   See https://mongodb.com/docs/manual/reference/command/listDatabases/
    #   for more information and usage.
    # @option opts [ Session ] :session The session to use.
    # @option options [ Object ] :comment A user-provided
    #   comment to attach to this command.
    #
    # @return [ Array<String> ] The names of the databases.
    #
    # @since 2.0.5
    def database_names(filter = {}, opts = {})
      list_databases(filter, true, opts).collect{ |info| info['name'] }
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
    # @option opts [ true, false ] :authorized_databases A flag that determines
    #   which databases are returned based on user privileges when access control
    #   is enabled
    #
    #   See https://mongodb.com/docs/manual/reference/command/listDatabases/
    #   for more information and usage.
    # @option opts [ Session ] :session The session to use.
    # @option options [ Object ] :comment A user-provided
    #   comment to attach to this command.
    #
    # @return [ Array<Hash> ] The info for each database.
    #
    # @since 2.0.5
    def list_databases(filter = {}, name_only = false, opts = {})
      cmd = { listDatabases: 1 }
      cmd[:nameOnly] = !!name_only
      cmd[:filter] = filter unless filter.empty?
      cmd[:authorizedDatabases] = true if opts[:authorized_databases]
      use(Database::ADMIN).database.read_command(cmd, opts).first[Database::DATABASES]
    end

    # Returns a list of Mongo::Database objects.
    #
    # @example Get a list of Mongo::Database objects.
    #   client.list_mongo_databases
    #
    # @param [ Hash ] filter The filter criteria for getting a list of databases.
    # @param [ Hash ] opts The command options.
    #
    # @option opts [ Session ] :session The session to use.
    # @option options [ Object ] :comment A user-provided
    #   comment to attach to this command.
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
      session = get_session!(options.merge(implicit: false))
      if block_given?
        begin
          yield session
        ensure
          session.end_session
        end
      else
        session
      end
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
    # @option options [ Object ] :comment A user-provided
    #   comment to attach to this command.
    # @option options [ Boolean ] :show_expanded_events Enables the server to
    #   send the 'expanded' list of change stream events. The list of additional
    #   events included with this flag set are: createIndexes, dropIndexes,
    #   modify, create, shardCollection, reshardCollection,
    #   refineCollectionShardKey.
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

      view_options = options.dup
      view_options[:await_data] = true if options[:max_await_time_ms]

      Mongo::Collection::View::ChangeStream.new(
        Mongo::Collection::View.new(self["#{Database::COMMAND}.aggregate"], {}, view_options),
        pipeline,
        Mongo::Collection::View::ChangeStream::CLUSTER,
        options)
    end

    # Returns a session to use for operations if possible.
    #
    # If :session option is set, validates that session and returns it.
    # Otherwise, if deployment supports sessions, creates a new session and
    # returns it. When a new session is created, the session will be implicit
    # (lifecycle is managed by the driver) if the :implicit option is given,
    # otherwise the session will be explicit (lifecycle managed by the
    # application). If deployment does not support session, returns nil.
    #
    # @option options [ true | false ] :implicit When no session is passed in,
    #   whether to create an implicit session.
    # @option options [ Session ] :session The session to validate and return.
    #
    # @return [ Session | nil ] Session object or nil if sessions are not
    #   supported by the deployment.
    #
    # @api private
    def get_session(options = {})
      get_session!(options)
    rescue Error::SessionsNotSupported
      nil
    end

    # Creates a session to use for operations if possible and yields it to
    # the provided block.
    #
    # If :session option is set, validates that session and uses it.
    # Otherwise, if deployment supports sessions, creates a new session and
    # uses it. When a new session is created, the session will be implicit
    # (lifecycle is managed by the driver) if the :implicit option is given,
    # otherwise the session will be explicit (lifecycle managed by the
    # application). If deployment does not support session, yields nil to
    # the block.
    #
    # When the block finishes, if the session was created and was implicit,
    # or if an implicit session was passed in, the session is ended which
    # returns it to the pool of available sessions.
    #
    # @option options [ true | false ] :implicit When no session is passed in,
    #   whether to create an implicit session.
    # @option options [ Session ] :session The session to validate and return.
    #
    # @api private
    def with_session(options = {}, &block)
      # TODO: Add this back in RUBY-3174.
      # assert_not_closed

      session = get_session(options)

      yield session
    ensure
      if session && session.implicit?
        session.end_session
      end
    end

    class << self
      # Lowercases auth mechanism properties, if given, in the specified
      # options, then converts the options to an instance of Options::Redacted.
      #
      # @api private
      def canonicalize_ruby_options(options)
        Options::Redacted.new(Hash[options.map do |k, v|
          if k == :auth_mech_properties || k == 'auth_mech_properties'
            if v
              v = Hash[v.map { |pk, pv| [pk.downcase, pv] }]
            end
          end
          [k, v]
        end])
      end
    end

    # Returns encrypted field map hash if provided when creating the client.
    #
    # @return [ Hash | nil ] Encrypted field map hash, or nil if not set.
    # @api private
    def encrypted_fields_map
      @encrypted_fields_map ||= @options.fetch(:auto_encryption_options, {})[:encrypted_fields_map]
    end

    private

    # Create a new encrypter object using the client's auto encryption options
    def build_encrypter
      @encrypter = Crypt::AutoEncrypter.new(
        @options[:auto_encryption_options].merge(client: self)
      )
    end

    # Generate default client options based on the URI and options
    # passed into the Client constructor.
    def default_options(options)
      Database::DEFAULT_OPTIONS.dup.tap do |default_options|
        if options[:auth_mech] || options[:user]
          default_options[:auth_source] = Auth::User.default_auth_source(options)
        end

        if options[:auth_mech] == :gssapi
          default_options[:auth_mech_properties] = { service_name: 'mongodb' }
        end

        default_options[:retry_reads] = true
        default_options[:retry_writes] = true
      end
    end

    # Implementation for #close, assumes the connect lock is already acquired.
    def do_close
      @cluster.close
      close_encrypter
    end

    # Returns a session to use for operations.
    #
    # If :session option is set, validates that session and returns it.
    # Otherwise, if deployment supports sessions, creates a new session and
    # returns it. When a new session is created, the session will be implicit
    # (lifecycle is managed by the driver) if the :implicit option is given,
    # otherwise the session will be explicit (lifecycle managed by the
    # application). If deployment does not support session, raises
    # Error::InvalidSession.
    #
    # @option options [ true | false ] :implicit When no session is passed in,
    #   whether to create an implicit session.
    # @option options [ Session ] :session The session to validate and return.
    #
    # @return [ Session ] A session object.
    #
    # @raise Error::SessionsNotSupported if sessions are not supported by
    #   the deployment.
    #
    # @api private
    def get_session!(options = {})
      if options[:session]
        return options[:session].validate!(self)
      end

      cluster.validate_session_support!

      options = {implicit: true}.update(options)

      server_session = if options[:implicit]
        nil
      else
        cluster.session_pool.checkout
      end

      Session.new(server_session, self, options)
    end

    # Auxiliary method that is called by interpreter when copying the client
    # via dup or clone.
    #
    # @param [ Mongo::Client ] original Client that is being cloned.
    #
    # @api private
    def initialize_copy(original)
      @options = original.options.dup
      @connect_lock = Mutex.new
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

    # Validates options in the provided argument for validity.
    # The argument may contain a subset of options that the client will
    # eventually have; this method validates each of the provided options
    # but does not check for interactions between combinations of options.
    def validate_new_options!(opts)
      return Options::Redacted.new unless opts
      if opts[:read_concern]
        # Raise an error for non user-settable options
        if opts[:read_concern][:after_cluster_time]
          raise Mongo::Error::InvalidReadConcern.new(
            'The after_cluster_time read_concern option cannot be specified by the user'
          )
        end

        given_keys = opts[:read_concern].keys.map(&:to_s)
        allowed_keys = ['level']
        invalid_keys = given_keys - allowed_keys
        # Warn that options are invalid but keep it and forward to the server
        unless invalid_keys.empty?
          log_warn("Read concern has invalid keys: #{invalid_keys.join(',')}.")
        end
      end

      if server_api = opts[:server_api]
        unless server_api.is_a?(Hash)
          raise ArgumentError, ":server_api value must be a hash: #{server_api}"
        end

        extra_keys = server_api.keys - %w(version strict deprecation_errors)
        unless extra_keys.empty?
          raise ArgumentError, "Unknown keys under :server_api: #{extra_keys.map(&:inspect).join(', ')}"
        end

        if version = server_api[:version]
          unless VALID_SERVER_API_VERSIONS.include?(version)
            raise ArgumentError, "Unknown server API version: #{version}"
          end
        end
      end

      Lint.validate_underscore_read_preference(opts[:read])
      Lint.validate_read_concern_option(opts[:read_concern])
      opts.each.inject(Options::Redacted.new) do |_options, (k, v)|
        key = k.to_sym
        if VALID_OPTIONS.include?(key)
          validate_max_min_pool_size!(key, opts)
          validate_max_connecting!(key, opts)
          validate_read!(key, opts)
          if key == :compressors
            compressors = valid_compressors(v)

            if compressors.include?('snappy')
              validate_snappy_compression!
            end

            if compressors.include?('zstd')
              validate_zstd_compression!
            end

            _options[key] = compressors unless compressors.empty?
          elsif key == :srv_max_hosts
            if v && (!v.is_a?(Integer) || v < 0)
              log_warn("#{v} is not a valid integer for srv_max_hosts")
            else
              _options[key] = v
            end
          else
            _options[key] = v
          end
        else
          log_warn("Unsupported client option '#{k}'. It will be ignored.")
        end
        _options
      end
    end

    # Validates all options after they are set on the client.
    # This method is intended to catch combinations of options which are
    # not allowed.
    def validate_options!(addresses = nil, is_srv: nil)
      if options[:write] && options[:write_concern] && options[:write] != options[:write_concern]
        raise ArgumentError, "If :write and :write_concern are both given, they must be identical: #{options.inspect}"
      end

      connect = options[:connect]&.to_sym

      if connect && !%i(direct replica_set sharded load_balanced).include?(connect)
        raise ArgumentError, "Invalid :connect option value: #{connect}"
      end

      if options[:direct_connection]
        if connect && connect != :direct
          raise ArgumentError, "Conflicting client options: direct_connection=true and connect=#{connect}"
        end
        # When a new client is created, we get the list of seed addresses
        if addresses && addresses.length > 1
          raise ArgumentError, "direct_connection=true cannot be used with multiple seeds"
        end
        # When a client is copied using #with, we have a cluster
        if cluster && !cluster.topology.is_a?(Mongo::Cluster::Topology::Single)
          raise ArgumentError, "direct_connection=true cannot be used with topologies other than Single (this client is #{cluster.topology.class.name.sub(/.*::/, '')})"
        end
      end

      if options[:load_balanced]
        if addresses && addresses.length > 1
          raise ArgumentError, "load_balanced=true cannot be used with multiple seeds"
        end

        if options[:direct_connection]
          raise ArgumentError, "direct_connection=true cannot be used with load_balanced=true"
        end

        if connect && connect != :load_balanced
          raise ArgumentError, "connect=#{connect} cannot be used with load_balanced=true"
        end

        if options[:replica_set]
          raise ArgumentError, "load_balanced=true cannot be used with replica_set option"
        end
      end

      if connect == :load_balanced
        if addresses && addresses.length > 1
          raise ArgumentError, "connect=load_balanced cannot be used with multiple seeds"
        end

        if options[:replica_set]
          raise ArgumentError, "connect=load_balanced cannot be used with replica_set option"
        end
      end

      if options[:direct_connection] == false && connect && connect == :direct
        raise ArgumentError, "Conflicting client options: direct_connection=false and connect=#{connect}"
      end

      %i(connect_timeout socket_timeout).each do |key|
        if value = options[key]
          unless Numeric === value
            raise ArgumentError, "#{key} must be a non-negative number: #{value}"
          end
          if value < 0
            raise ArgumentError, "#{key} must be a non-negative number: #{value}"
          end
        end
      end

      if value = options[:bg_error_backtrace]
        case value
        when Integer
          if value <= 0
            raise ArgumentError, ":bg_error_backtrace option value must be true, false, nil or a positive integer: #{value}"
          end
        when true
          # OK
        else
          raise ArgumentError, ":bg_error_backtrace option value must be true, false, nil or a positive integer: #{value}"
        end
      end

      if libraries = options[:wrapping_libraries]
        unless Array === libraries
          raise ArgumentError, ":wrapping_libraries must be an array of hashes: #{libraries}"
        end

        libraries = libraries.map do |library|
          Utils.shallow_symbolize_keys(library)
        end

        libraries.each do |library|
          unless Hash === library
            raise ArgumentError, ":wrapping_libraries element is not a hash: #{library}"
          end

          if library.empty?
            raise ArgumentError, ":wrapping_libraries element is empty"
          end

          unless (library.keys - %i(name platform version)).empty?
            raise ArgumentError, ":wrapping_libraries element has invalid keys (allowed keys: :name, :platform, :version): #{library}"
          end

          library.each do |key, value|
            if value.include?('|')
              raise ArgumentError, ":wrapping_libraries element value cannot include '|': #{value}"
            end
          end
        end
      end

      if options[:srv_max_hosts] && options[:srv_max_hosts] > 0
        if options[:replica_set]
          raise ArgumentError, ":srv_max_hosts > 0 cannot be used with :replica_set option"
        end

        if options[:load_balanced]
          raise ArgumentError, ":srv_max_hosts > 0 cannot be used with :load_balanced=true"
        end
      end

      unless is_srv.nil? || is_srv
        if options[:srv_max_hosts]
          raise ArgumentError, ":srv_max_hosts cannot be used on non-SRV URI"
        end

        if options[:srv_service_name]
          raise ArgumentError, ":srv_service_name cannot be used on non-SRV URI"
        end
      end
    end

    # Validates all authentication-related options after they are set on the client
    # This method is intended to catch combinations of options which are not allowed
    def validate_authentication_options!
      auth_mech = options[:auth_mech]
      user = options[:user]
      password = options[:password]
      auth_source = options[:auth_source]
      mech_properties = options[:auth_mech_properties]

      if auth_mech.nil?
        if user && user.empty?
          raise Mongo::Auth::InvalidConfiguration, 'Empty username is not supported for default auth mechanism'
        end

        if auth_source == ''
          raise Mongo::Auth::InvalidConfiguration, 'Auth source cannot be empty for default auth mechanism'
        end

        return
      end

      if !Mongo::Auth::SOURCES.key?(auth_mech)
        raise Mongo::Auth::InvalidMechanism.new(auth_mech)
      end

      if user.nil? && !%i(aws mongodb_x509).include?(auth_mech)
        raise Mongo::Auth::InvalidConfiguration, "Username is required for auth mechanism #{auth_mech}"
      end

      if password.nil? && !%i(aws gssapi mongodb_x509).include?(auth_mech)
        raise Mongo::Auth::InvalidConfiguration, "Password is required for auth mechanism #{auth_mech}"
      end

      if password && auth_mech == :mongodb_x509
        raise Mongo::Auth::InvalidConfiguration, 'Password is not supported for :mongodb_x509 auth mechanism'
      end

      if auth_mech == :aws && user && !password
        raise Mongo::Auth::InvalidConfiguration, 'Username is provided but password is not provided for :aws auth mechanism'
      end

      if %i(aws gssapi mongodb_x509).include?(auth_mech)
        if !['$external', nil].include?(auth_source)
          raise Mongo::Auth::InvalidConfiguration, "#{auth_source} is an invalid auth source for #{auth_mech}; valid options are $external and nil"
        end
      else
        # Auth source is the database name, and thus cannot be the empty string.
        if auth_source == ''
          raise Mongo::Auth::InvalidConfiguration, "Auth source cannot be empty for auth mechanism #{auth_mech}"
        end
      end

      if mech_properties && !%i(aws gssapi).include?(auth_mech)
        raise Mongo::Auth::InvalidConfiguration, ":mechanism_properties are not supported for auth mechanism #{auth_mech}"
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

    def validate_snappy_compression!
      return if defined?(Snappy)
      require 'snappy'
    rescue LoadError => e
      raise Error::UnmetDependency, "Cannot enable snappy compression because the snappy gem " \
        "has not been installed. Add \"gem 'snappy'\" to your Gemfile and run " \
        "\"bundle install\" to install the gem. (#{e.class}: #{e})"
    end

    def validate_zstd_compression!
      return if defined?(Zstd)
      require 'zstd-ruby'
    rescue LoadError => e
      raise Error::UnmetDependency, "Cannot enable zstd compression because the zstd-ruby gem " \
        "has not been installed. Add \"gem 'zstd-ruby'\" to your Gemfile and run " \
        "\"bundle install\" to install the gem. (#{e.class}: #{e})"
    end

    def validate_max_min_pool_size!(option, opts)
      if option == :min_pool_size && opts[:min_pool_size]
        max = opts[:max_pool_size] || Server::ConnectionPool::DEFAULT_MAX_SIZE
        if max != 0 && opts[:min_pool_size] > max
          raise Error::InvalidMinPoolSize.new(opts[:min_pool_size], max)
        end
      end
      true
    end

    # Validates whether the max_connecting option is valid.
    #
    # @param [ Symbol ] option The option to validate.
    # @param [ Hash ] opts The client options.
    #
    # @return [ true ] If the option is valid.
    # @raise [ Error::InvalidMaxConnecting ] If the option is invalid.
    def validate_max_connecting!(option, opts)
      if option == :max_connecting && opts.key?(:max_connecting)
        max_connecting = opts[:max_connecting] || Server::ConnectionPool::DEFAULT_MAX_CONNECTING
        if max_connecting <= 0
          raise Error::InvalidMaxConnecting.new(opts[:max_connecting])
        end
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
          raise Error::InvalidReadOption.new(read, "the read preference must be specified as a hash: { mode: #{read.inspect} }")
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

    def assert_not_closed
      if closed?
        raise Error::ClientClosed, "The client was closed and is not usable for operations. Call #reconnect to reset this client instance or create a new client instance"
      end
    end
  end
end

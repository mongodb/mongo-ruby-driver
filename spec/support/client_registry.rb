require 'singleton'

module Mongo
  class Client
    alias :initialize_without_registry :initialize
    def initialize(addresses_or_uri, options = nil, &block)
      options = options ? options.dup : {}
      id = "registry:#{SecureRandom.uuid}"
      bt = begin
        x
      rescue => e
        e.backtrace.join("\n")
      end
      puts "--- client #{id}"
      puts bt
      puts '--- client #{id} end'
      #options[:app_name] ||= id
      initialize_without_registry(addresses_or_uri, options, &block)
    end

    alias :with_without_registry :with
    def with(*args)
      with_without_registry(*args).tap do |client|
        ClientRegistry.instance.register_local_client(client)
      end
    end
  end
end

# Test suite uses a number of global clients (with lifetimes spanning
# several tests) as well as local clients (created for a single example
# or shared across several examples in one example group).
#
# To make client cleanup easy, all local clients are automatically closed
# after every example. This means there is no need for an individual example
# to worry about closing its local clients.
#
# In SDAM tests, having global clients is problematic because mocks can
# be executed on global clients instead of the local clients.
# To address this, tests can close all global clients. This kills monitoring
# threads and SDAM on the global clients. Later tests that need one of the
# global clients will have the respective global client reconnected
# automatically by the client registry.
#
# Lastly, Client#with sometimes creates a new cluster and sometimes reuses
# the cluster on the receiver. Client registry patches Mongo::Client to
# track all clients returned by Client#with, and considers these clients
# local to the example being run. This means global clients should not be
# created via #with. Being local clients, clients created by #with will be
# automatically closed after each example. If these clients shared their
# cluster with a global client, this will make the global client not do
# SDAM anymore; this situation is automatically fixed by the client registry
# when a subsequent test requests the global client in question.
class ClientRegistry
  include Singleton

  def initialize
    # clients local to an example.
    # any clients in @clients can be closed in an after hook
    @local_clients = []
    # clients global to the test suite, should not be closed in an after hooks
    # but their monitoring may need to be suspended/resumed
    @global_clients = {}

    # JRuby appears to somehow manage to access client registry concurrently
    @lock = Mutex.new
  end

  class << self
    def client_perished?(client)
      if !client.cluster.connected?
        true
      else
        perished = false
        client.cluster.servers_list.each do |server|
          thread = server.monitor.instance_variable_get('@thread')
          if thread.nil? || !thread.alive?
            perished = true
          end
        end
        perished
      end
    end
    private :client_perished?

    def reconnect_client_if_perished(client)
      if client_perished?(client)
        client.reconnect
      end
    end
  end

  def global_client(name)
    if client = @global_clients[name]
      self.class.reconnect_client_if_perished(client)
      return client
    end

    @global_clients[name] = new_global_client(name)
  end

  def new_global_client(name)
    case name
    # Provides a basic scanned client to do an ismaster check.
    when 'basic'
      Mongo::Client.new(
        SpecConfig.instance.addresses,
        SpecConfig.instance.test_options.merge(database: SpecConfig.instance.test_db),
      )
    # Provides an unauthorized mongo client on the default test database.
    when 'unauthorized'
      Mongo::Client.new(
        SpecConfig.instance.addresses,
        SpecConfig.instance.test_options.merge(database: SpecConfig.instance.test_db),
      )
    # Provides an authorized mongo client on the default test database for the
    # default test user.
    when 'authorized'
      client_options = {
        database: SpecConfig.instance.test_db,
      }.update(SpecConfig.instance.credentials_or_external_user(
        user: SpecConfig.instance.test_user.name,
        password: SpecConfig.instance.test_user.password,
        auth_source: 'admin',
      ))

      Mongo::Client.new(
        SpecConfig.instance.addresses,
        SpecConfig.instance.test_options.merge(client_options)
      )
    # Provides an authorized mongo client that retries writes.
    when 'authorized_with_retry_writes'
      global_client('authorized').with(
        retry_writes: true,
        server_selection_timeout: 4.97,
      )
    # Provides an authorized mongo client that uses legacy read retry logic.
    when 'authorized_without_retry_reads'
      global_client('authorized').with(
        retry_reads: false,
        server_selection_timeout: 4.27,
      )
    # Provides an authorized mongo client that does not retry reads at all.
    when 'authorized_without_any_retry_reads'
      global_client('authorized').with(
        retry_reads: false, max_read_retries: 0,
        server_selection_timeout: 4.27,
      )
    # Provides an authorized mongo client that does not retry writes,
    # overriding global test suite option to retry writes if necessary.
    when 'authorized_without_retry_writes'
      global_client('authorized').with(
        retry_writes: false,
        server_selection_timeout: 4.99,
      )
    # Provides an authorized mongo client that does not retry writes
    # using either modern or legacy mechanisms.
    when 'authorized_without_any_retry_writes'
      global_client('authorized').with(
        retry_writes: false, max_write_retries: 0,
        server_selection_timeout: 4.99,
      )
    # Provides an authorized mongo client that does not retry reads or writes
    # at all.
    when 'authorized_without_any_retries'
      global_client('authorized').with(
        retry_reads: false, max_read_retries: 0,
        retry_writes: false, max_write_retries: 0,
        server_selection_timeout: 4.27,
      )
    # Provides an unauthorized mongo client on the admin database, for use in
    # setting up the first admin root user.
    when 'admin_unauthorized'
      Mongo::Client.new(
        SpecConfig.instance.addresses,
        SpecConfig.instance.test_options.merge(
          database: Mongo::Database::ADMIN,
          monitoring: false),
      )
    # Get an authorized client on the test database logged in as the admin
    # root user.
    when 'root_authorized'
      if SpecConfig.instance.x509_auth?
        client_options = SpecConfig.instance.auth_options.merge(
          database: SpecConfig.instance.test_db,
        )
      else
        client_options = {
          database: SpecConfig.instance.test_db,
        }.update(SpecConfig.instance.credentials_or_external_user(
          user: SpecConfig.instance.root_user.name,
          password: SpecConfig.instance.root_user.password,
          auth_source: SpecConfig.instance.auth_source || Mongo::Database::ADMIN,
        ))
      end

      Mongo::Client.new(
        SpecConfig.instance.addresses,
        SpecConfig.instance.test_options.merge(client_options),
      )
    else
      raise "Don't know how to construct global client #{name}"
    end
  end
  private :new_global_client

  def new_local_client(*args)
    Mongo::Client.new(*args).tap do |client|
      @lock.synchronize do
        @local_clients << client
      end
    end
  end

  def register_local_client(client)
    @lock.synchronize do
      @local_clients << client
    end
    client
  end

  def close_local_clients
    @lock.synchronize do
      @local_clients.each do |client|
        # If this client shares cluster with any of the global clients,
        # do not disconnect the cluster so that the global clients continue
        # working into the next test(s).
        # If this client does not share cluster with any global clients,
        # this client can be closed completely via the #close method.
        #
        # Clients can also have slaved auto encryption objects (mongocryptd
        # client and key vault client) which also need to be cleaned up.
        # These slaved objects are always unique to the client which hosts
        # them - they are never shared between clients. Therefore, we
        # always tear down encryption objects for each local client here.
        # This is done either as part of #close if #close is invoked, or
        # explicitly if #close is not invoked due to cluster sharing.
        cluster = client.cluster
        if @global_clients.none? { |name, global_client|
          cluster.object_id == global_client.cluster.object_id
        }
          # Cluster not shared, disconnect cluster and clean up encryption.
          client.close
        else
          # Cluster is shared, clean up encryption only.
          client.close_encrypter
        end
      end

      @local_clients = []
    end
  end

  def close_all_clients
    ClusterTools.instance.close_clients
    close_local_clients
    @lock.synchronize do
      @global_clients.each do |name, client|
        client.close
      end
    end
  end
end

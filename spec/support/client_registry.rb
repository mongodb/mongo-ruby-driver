require 'singleton'

module Mongo
  class Client
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
  end

  def global_client(name)
    if client = @global_clients[name]
      if !client.cluster.connected?
        reconnect = true
      else
        reconnect = false
        client.cluster.servers_list.each do |server|
          thread = server.monitor.instance_variable_get('@thread')
          if thread.nil? || !thread.alive?
            reconnect = true
          end
        end
      end
      if reconnect
        client.reconnect
      end
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
        SpecConfig.instance.test_options.merge(database: SpecConfig.instance.test_db, monitoring: false),
      )
    # Provides an authorized mongo client on the default test database for the
    # default test user.
    when 'authorized'
      Mongo::Client.new(
        SpecConfig.instance.addresses,
        SpecConfig.instance.test_options.merge(
          database: SpecConfig.instance.test_db,
          user: SpecConfig.instance.test_user.name,
          password: SpecConfig.instance.test_user.password),
      )
    # Provides an authorized mongo client that retries writes.
    when 'authorized_with_retry_writes'
      global_client('authorized').with(
        retry_writes: true,
        server_selection_timeout: 4.97,
      ).tap do |client|
        client.subscribe(Mongo::Monitoring::COMMAND, EventSubscriber)
      end
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
      ).tap do |client|
        client.subscribe(Mongo::Monitoring::COMMAND, EventSubscriber)
      end
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
      Mongo::Client.new(
        SpecConfig.instance.addresses,
        SpecConfig.instance.test_options.merge(
          user: SpecConfig.instance.root_user.name,
          password: SpecConfig.instance.root_user.password,
          database: SpecConfig.instance.test_db,
          auth_source: SpecConfig.instance.auth_source || Mongo::Database::ADMIN,
          monitoring: false
        ),
      )
    # Get an authorized client on the admin database logged in as the admin
    # root user.
    when 'root_authorized_admin'
      Mongo::Client.new(
        SpecConfig.instance.addresses,
        SpecConfig.instance.test_options.merge(
          user: SpecConfig.instance.root_user.name,
          password: SpecConfig.instance.root_user.password,
          database: 'admin',
          auth_source: SpecConfig.instance.auth_source || Mongo::Database::ADMIN,
          monitoring: false
        ),
      )
    # A client that has an event subscriber for commands.
    when 'subscribed'
      Mongo::Client.new(
        SpecConfig.instance.addresses,
        SpecConfig.instance.test_options.merge(
          database: SpecConfig.instance.test_db,
          user: SpecConfig.instance.test_user.name,
          password: SpecConfig.instance.test_user.password),
      ).tap do |client|
        client.subscribe(Mongo::Monitoring::COMMAND, EventSubscriber)
      end
    else
      raise "Don't know how to construct global client #{name}"
    end
  end
  private :new_global_client

  def new_local_client(*args)
    Mongo::Client.new(*args).tap do |client|
      @local_clients << client
    end
  end

  def register_local_client(client)
    @local_clients << client
    client
  end

  def close_local_clients
    @local_clients.map(&:close)
    @local_clients = []
  end

  def close_all_clients
    close_local_clients
    @global_clients.each do |name, client|
      client.close(true)
    end
  end
end

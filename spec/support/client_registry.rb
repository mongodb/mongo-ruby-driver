require 'singleton'

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
      unless client.cluster.connected?
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
      global_client('authorized').with(retry_writes: true).tap do |client|
        client.subscribe(Mongo::Monitoring::COMMAND, EventSubscriber)
      end
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

  def close_local_clients
    @local_clients.map(&:close)
    @local_clients = []
  end

  def close_all_clients
    close_local_clients
    @global_clients.each do |name, client|
      client.close
    end
  end
end

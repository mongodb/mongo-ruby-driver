require 'singleton'

class ClientRegistry
  include Singleton

  def initialize
    # clients local to an example.
    # any clients in @clients can be closed in an after hook
    @local_clients = []
    # clients global to the test suite, should not be closed in an after hooks
    # but their monitoring may need to be suspended/resumed
    @global_clients = []
  end

  def new_global_client(*args)
    Mongo::Client.new(*args).tap do |client|
      @global_clients << client
    end
  end

  def new_local_client(*args)
    Mongo::Client.new(*args).tap do |client|
      @local_clients << client
    end
  end

  def close_local_clients
    @local_clients.map(&:close)
    @local_clients = []
  end
end

module Mongo
  class SharedConnectionPool
    MUTEX = Mutex.new
    @pools = {}

    def get(server)
      MUTEX.synchronize do
        pools[server.address] ||= Server::ConnectionPool.get(server)
      end
    end
  end
end

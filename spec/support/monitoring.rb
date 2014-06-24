module Mongo
  class Cluster

    # Force a scan of all servers in the cluster.
    #
    # @api test
    #
    # @example Scan the cluster.
    #   cluster.scan!
    #
    # @note This is for testing purposes only.
    #
    # @return [ true ] Always true if no error.
    #
    # @since 2.0.0
    def scan!
      @servers.each{ |server| server.check! } and true
    end
  end

  class Server

    # Tells the monitor to immediately check the server status.
    #
    # @api test
    #
    # @example Check the server status.
    #   server.check!
    #
    # @note Used for testing purposes.
    #
    # @return [ Server::Description ] The updated server description.
    #
    # @since 2.0.0
    def check!
      @monitor.check!
    end

    # In the test suite we don't need the monitor to run.
    def initialize(address, options = {})
      @address = Address.new(address)
      @options = options
      @mutex = Mutex.new
      @monitor = Monitor.new(self, options)
      @description = Description.new(self)
    end

    class Monitor

      # We do synchronous scans in the test suite so need to expose the ability
      # to do it in the monitor.
      def check!
        server.description.update!(*ismaster)
      end
    end
  end
end

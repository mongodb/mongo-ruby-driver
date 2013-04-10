module Mongo
  class Pool
    PING_ATTEMPTS  = 6
    MAX_PING_TIME  = 1_000_000
    PRUNE_INTERVAL = 10_000

    attr_accessor :host,
                  :port,
                  :address,
                  :size,
                  :timeout,
                  :checked_out,
                  :client,
                  :node

    # Create a new pool of connections.
    def initialize(client, host, port, opts={})
      @client = client

      @host, @port = host, port

      # A Mongo::Node object.
      @node = opts[:node]

      # The string address
      @address = "#{@host}:#{@port}"

      # Pool size and timeout.
      @size    = opts.fetch(:size, 20)
      @timeout = opts.fetch(:timeout, 30)

      # Mutex for synchronizing pool access
      @connection_mutex = Mutex.new

      # Mutex for synchronizing pings
      @ping_mutex = Mutex.new

      # Condition variable for signal and wait
      @queue = ConditionVariable.new

      # Operations to perform on a socket
      @socket_ops = Hash.new { |h, k| h[k] = [] }

      @sockets               = []
      @checked_out           = []
      @ping_time             = nil
      @last_ping             = nil
      @closed                = false
      @thread_ids_to_sockets = {}
      @checkout_counter      = 0
    end

    # Close this pool.
    #
    # @option opts [Boolean]:soft (false) If true,
    #   close only those sockets that are not checked out.
    def close(opts={})
      @connection_mutex.synchronize do
        if opts[:soft] && !@checked_out.empty?
          @closing = true
          close_sockets(@sockets - @checked_out)
        else
          close_sockets(@sockets)
          @closed = true
        end
        @node.close if @node
      end
      true
    end

    def tags
      @node.tags
    end

    def healthy?
      close if @sockets.all?(&:closed?)
      !closed? && node.healthy?
    end

    def closed?
      @closed
    end

    def up?
      !@closed
    end

    def inspect
      "#<Mongo::Pool:0x#{self.object_id.to_s(16)} @host=#{@host} @port=#{port} " +
        "@ping_time=#{@ping_time} #{@checked_out.size}/#{@size} sockets available " +
        "up=#{!closed?}>"
    end

    def host_string
      "#{@host}:#{@port}"
    end

    def host_port
      [@host, @port]
    end

    # Refresh ping time only if we haven't
    # checked within the last five minutes.
    def ping_time
      @ping_mutex.synchronize do
        if !@last_ping || (Time.now - @last_ping) > 300
          @ping_time = refresh_ping_time
          @last_ping = Time.now
        end
      end
      @ping_time
    end

    # Return the time it takes on average
    # to do a round-trip against this node.
    def refresh_ping_time
      trials = []
      PING_ATTEMPTS.times do
        t1 = Time.now
        if !self.ping
          return MAX_PING_TIME
        end
        trials << (Time.now - t1) * 1000
      end

      trials.sort!

      # Delete shortest and longest times
      trials.delete_at(trials.length-1)
      trials.delete_at(0)

      total = 0.0
      trials.each { |t| total += t }

      (total / trials.length).ceil
    end

    def ping
      begin
        return self.client['admin'].command({:ping => 1}, :socket => @node.socket, :timeout => MAX_PING_TIME)
      rescue ConnectionFailure, OperationFailure, SocketError, SystemCallError, IOError
        return false
      end
    end

    # Return a socket to the pool.
    def checkin(socket)
      @connection_mutex.synchronize do
        if @checked_out.delete(socket)
          @queue.broadcast
        else
          return false
        end
      end
      true
    end

    # Adds a new socket to the pool and checks it out.
    #
    # This method is called exclusively from #checkout;
    # therefore, it runs within a mutex.
    def checkout_new_socket
      begin
        socket = @client.socket_class.new(@host, @port, @client.op_timeout)
        socket.pool = self
      rescue => ex
        socket.close if socket
        @node.close if @node
        raise ConnectionFailure, "Failed to connect to host #{@host} and port #{@port}: #{ex}"
      end

      # If any saved authentications exist, we want to apply those
      # when creating new sockets.
      @client.apply_saved_authentication(:socket => socket)

      @sockets << socket
      @checked_out << socket
      @thread_ids_to_sockets[Thread.current.object_id] = socket
      socket
    end

    # If a user calls DB#authenticate, and several sockets exist,
    # then we need a way to apply the authentication on each socket.
    # So we store the apply_authentication method, and this will be
    # applied right before the next use of each socket.
    def authenticate_existing
      @connection_mutex.synchronize do
        @sockets.each do |socket|
          @socket_ops[socket] << Proc.new do
            @client.apply_saved_authentication(:socket => socket)
          end
        end
      end
    end

    # Store the logout op for each existing socket to be applied before
    # the next use of each socket.
    def logout_existing(db)
      @connection_mutex.synchronize do
        @sockets.each do |socket|
          @socket_ops[socket] << Proc.new do
            @client.db(db).issue_logout(:socket => socket)
          end
        end
      end
    end

    # Checks out the first available socket from the pool.
    #
    # If the pid has changed, remove the socket and check out
    # new one.
    #
    # This method is called exclusively from #checkout;
    # therefore, it runs within a mutex.
    def checkout_existing_socket(socket=nil)
      if !socket
        available = @sockets - @checked_out
        socket = available[rand(available.length)]
      end

      if socket.pid != Process.pid
        @sockets.delete(socket)
        if socket
          socket.close unless socket.closed?
        end
        checkout_new_socket
      else
        @checked_out << socket
        @thread_ids_to_sockets[Thread.current.object_id] = socket
        socket
      end
    end

    def prune_threads
      live_threads = Thread.list.map(&:object_id)
      @thread_ids_to_sockets.reject! do |key, value|
        !live_threads.include?(key)
      end
    end

    def check_prune
      if @checkout_counter > PRUNE_INTERVAL
          @checkout_counter = 0
          prune_threads
      else
        @checkout_counter += 1
      end
    end

    # Check out an existing socket or create a new socket if the maximum
    # pool size has not been exceeded. Otherwise, wait for the next
    # available socket.
    def checkout
      @client.connect if !@client.connected?
      start_time = Time.now
      loop do
        if (Time.now - start_time) > @timeout
          raise ConnectionTimeoutError, "could not obtain connection within " +
            "#{@timeout} seconds. The max pool size is currently #{@size}; " +
            "consider increasing the pool size or timeout."
        end

        @connection_mutex.synchronize do
          check_prune
          socket = nil
          if socket_for_thread = @thread_ids_to_sockets[Thread.current.object_id]
            if !@checked_out.include?(socket_for_thread)
              socket = checkout_existing_socket(socket_for_thread)
            end
          else
            if @sockets.size < @size
              socket = checkout_new_socket
            elsif @checked_out.size < @sockets.size
              socket = checkout_existing_socket
            end
          end

          if socket
            # This calls all procs, in order, scoped to existing sockets.
            # At the moment, we use this to lazily authenticate and
            # logout existing socket connections.
            @socket_ops[socket].reject! do |op|
              op.call
            end

            if socket.closed?
              @checked_out.delete(socket)
              @sockets.delete(socket)
              @thread_ids_to_sockets.delete(Thread.current.object_id)
              socket = checkout_new_socket
            end

            return socket
          else
            # Otherwise, wait
            @queue.wait(@connection_mutex)
          end
        end
      end
    end

    private

    def close_sockets(sockets)
      sockets.each do |socket|
        @sockets.delete(socket)
        begin
          socket.close unless socket.closed?
        rescue IOError => ex
          warn "IOError when attempting to close socket connected to #{@host}:#{@port}: #{ex.inspect}"
        end
      end
    end

  end
end

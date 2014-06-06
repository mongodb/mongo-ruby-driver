# Copyright (C) 2009-2014 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Simulates cluster behaviour on the server side.
#
# @since 2.0.0
class ClusterSimulator

  # @return [ Array<Server> ] servers The servers in the cluster.
  attr_reader :servers

  # We pass through operations outside of ismaster to the proxied mongod
  # instance to get real results back.
  #
  # @return [ TCPSocket ] proxied_mongod The proxied instance.
  attr_reader :proxied_mongod

  # @return [ Manager ] manager The server manager.
  attr_reader :manager

  # Add another server to the replica set simulator.
  #
  # @example Add a server.
  #   simulator.add('127.0.0.1:27021')
  #
  # @param [ String ] seed The address to add.
  #
  # @since 2.0.0
  def add(seed)
    server = Server.new(self, seed)
    server.demote!
    server.start
    servers.push(server)
    self
  end

  # Demote a server to a secondary.
  #
  # @example Demote a server to a secondary.
  #   simulator.demote('127.0.0.1:27018')
  #
  # @param [ String ] seed The address of the server.
  #
  # @since 2.0.0
  def demote(seed)
    servers.each do |server|
      server.demote! if server.seed = seed
    end
    self
  end

  # Initialize the cluster simulator.
  #
  # @example Initialize the cluster simulator.
  #   ClusterSimulator.new([ '127.0.0.1:27018' ])
  #
  # @param [ Array<String> ] seeds The cluster seeds.
  #
  # @since 2.0.0
  def initialize(seeds)
    @proxied_mongod = TCPSocket.new('127.0.0.1', 27017)
    @servers = seeds.map{ |seed| Server.new(self, seed) }
    @manager = Manager.new(servers)
  end

  # Promote a server to primary.
  #
  # @example Promote a server to primary.
  #   simulator.promote('127.0.0.1:27018')
  #
  # @param [ String ] seed The address of the server.
  #
  # @since 2.0.0
  def promote(seed)
    servers.each do |server|
      server.seed = seed ? server.promote! : server.demote!
    end
    self
  end

  # Removes the server from the cluster simulator.
  #
  # @example Remove the server.
  #   simulator.remove('127.0.0.1:27019')
  #
  # @param [ String ] seed The server seed address.
  #
  # @since 2.0.0
  def remove(seed)
    server = servers.delete_if{ |server| server.seed == seed }.first
    if server
      server.stop
      manager.remove(server)
    end
    self
  end

  # Start the cluster simulator.
  #
  # @example Start the simulator.
  #   simulator.start
  #
  # @since 2.0.0
  def start
    primary, *secondaries = servers.shuffle
    primary.promote!
    secondaries.each(&:demote!)
    servers.each(&:start)
    Thread.start do
      Thread.abort_on_exception = true
      catch(:shutdown) do
        loop do
          server, client = manager.next_pair
          if server
            server.proxy(client, proxied_mongod)
          else
            Thread.pass
          end
        end
      end
    end
    self
  end

  # Stop the cluster simulator.
  #
  # @example Stop the cluster simulator
  #   simulator.stop
  #
  # @since 2.0.0
  def stop
    manager.shutdown
    servers.each(&:stop)
  end

  # Represents a server instance in the cluster simulator.
  #
  # @since 2.0.0
  class Server

    # Query op code.
    OP_QUERY = 2004

    # Getmore op code.
    OP_GETMORE = 2005

    # @return [ String ] seed The seed address
    attr_reader :seed

    # @return [ TCPServer ] server The underlying server.
    attr_reader :server

    # @return [ String ] host The server host.
    attr_reader :host

    # @return [ Integer ] port The server port.
    attr_reader :port

    # @return [ ClusterSimulator ] simulator The simulator.
    attr_reader :simulator

    def accept
      to_io.accept
    end

    # Close all the clients for this server.
    #
    # @example Close the clients.
    #   server.close_clients!
    #
    # @since 2.0.0
    def close_clients!
      simulator.manager.close_clients!(self)
    end

    # Is the server closed?
    #
    # @example Is the server closed?
    #   server.closed?
    #
    # @return [ true, false ] If the server is closed.
    #
    # @since 2.0.0
    def closed?
      !server || server.closed?
    end

    # Demote the server to secondary.
    #
    # @example Demote the server to secondary.
    #   server.demote!
    #
    # @since 2.0.0
    def demote!
      @primary, @secondary = false, true
      close_clients!
    end

    # Create the new server for the seed and simulator.
    #
    # @example Create the new server.
    #   ClusterSimulator::Server.new(simulator, '127.0.0.1:27018')
    #
    # @param [ ClusterSimulator ] simulator The simulator.
    # @param [ String ] seed The server seed address.
    #
    # @since 2.0.0
    def initialize(simulator, seed)
      @simulator, @seed, @primary, @secondary = simulator, seed, false, false
      host, port = seed.split(':')
      @host, @port = host, port.to_i
    end

    # Return a dummy ismaster command result based on this server's status in
    # the cluster simulator, as well as the others.
    #
    # @example Get the ismaster result.
    #   server.ismaster
    #
    # @return [ Mongo::Protocol::Reply ] The result of the ismaster command.
    #
    # @since 2.0.0
    def ismaster
      reply = Mongo::Protocol::Reply.new
      reply.instance_variable_set(:@flags, [])
      reply.instance_variable_set(:@cursor_id, 1)
      reply.instance_variable_set(:@starting_from, 1)
      reply.instance_variable_set(:@number_returned, 1)
      reply.instance_variable_set(:@documents, [
        {
          "ismaster" => primary?,
          "secondary" => secondary?,
          "hosts" => simulator.servers.map(&:seed),
          "me" => seed,
          "ok" => 1.0
        }
      ])
      reply
    end

    # Is this server the primary?
    #
    # @example Is the server primary?
    #   server.primary?
    #
    # @return [ true, false ] If the server is primary.
    #
    # @since 2.0.0
    def primary?
      @primary
    end

    # Promote this server to primary.
    #
    # @example Promote the server to primary.
    #   server.promote!
    #
    # @since 2.0.0
    def promote!
      @primary, @secomdary = true, false
      close_clients!
    end

    # Proxies a message from the client to the underlying mongod instance.
    #
    # @example Proxy a message.
    #   server.proxy(client, mongod)
    #
    # @param [ TCPSocket ] client The client connection.
    # @param [ TCPSocket ] mongod The underlying mongod connection.
    #
    # @since 2.0.0
    def proxy(client, mongod)
      message = client.read(16)
      length, op_code = message.unpack('l<x8l<')
      message << client.read(length - 16)

      if op_code == OP_QUERY && ismaster?(message)
        client.write(ismaster)
      else
        mongod.write(message)
        if op_code == OP_QUERY || op_code == OP_GETMORE
          outgoing = mongod.read(4)
          length, = outgoing.unpack('l<')
          outgoing << mongod.read(length - 4)
          client.write(outgoing)
        end
      end
    end

    # Is this server the secondary?
    #
    # @example Is the server secondary?
    #   server.secondary?
    #
    # @return [ true, false ] If the server is secondary.
    #
    # @since 2.0.0
    def secondary?
      @secondary
    end

    # Restart the server.
    #
    # @example Restart the server.
    #   server.restart
    #
    # @since 2.0.0
    def restart
      stop
      start
    end

    # Start the server.
    #
    # @example Start the server.
    #   server.start
    #
    # @return [ TCPServer ] The underlying TCPServer.
    #
    # @since 2.0.0
    def start
      @server = TCPServer.new(port)
    end

    # Stop the server.
    #
    # @example Stop the server.
    #   server.stop
    #
    # @since 2.0.0
    def stop
      if server
        close_clients!
        server.shutdown rescue nil
        server.close
        @server = nil
      end
    end

    alias :to_io :server

    private

    def ismaster?(incoming_message)
      data = StringIO.new(incoming_message)
      data.read(20) # header and flags
      data.gets("\x00") # collection name
      data.read(8) # skip/limit
      selector = BSON::Document.from_bson(data)
      selector == { 'ismaster' => 1 }
    end
  end

  # Manages all the servers in the cluster.
  #
  # @since 2.0.0
  class Manager

    # @return [ Array<TCPSocket> ] clients The clients.
    attr_reader :clients

    # @return [ Array<Server> ] servers The configured servers.
    attr_reader :servers

    # @return [ Float ] timeout The retry timeout.
    attr_reader :timeout

    # Add a server for the manager to manage.
    #
    # @example Add a server.
    #   manager.add(server)
    #
    # @param [ Server ] server The server to add.
    #
    # @since 2.0.0
    def add(server)
      servers.push(server)
    end

    # Closes all clients connected to the specific server.
    #
    # @example Close all the server's clients.
    #   manager.close_clients(server)
    #
    # @param [ Server ] server The server.
    #
    # @return [ true, false ] If clients were closed or not.
    #
    # @since 2.0.0
    def close_clients!(server)
      clients.reject! do |client|
        port = client.addr(false)[1]
        if port == server.port
          begin
            client.shutdown unless RUBY_PLATFORM =~ /java/
            client.close
          rescue; end; true
        else
          false
        end
      end
    end

    # Initialize the new simulator manager.
    #
    # @example Initialize the manager.
    #   Manager.new(servers)
    #
    # @param [ Array<Server> ] servers The servers to manage.
    #
    # @since 2.0.0
    def initialize(servers)
      @clients = []
      @servers = servers
      @shutdown = nil
      @timeout = 0.1
    end

    # Get the next client to send data to.
    #
    # @example Get the next server/client pair.
    #   manager.next_pair
    #
    # @return [ Array<Server, TCPSocket> ] The next server and associated
    #   client.
    #
    # @since 2.0.0
    def next_pair
      throw :shutdown if @shutdown

      begin
        available_servers = servers.reject(&:closed?)
        available_clients = clients.reject(&:closed?)
        readable, _, errors = Kernel.select(
          available_servers + available_clients, nil, available_clients, timeout
        )
      rescue IOError, Errno::EBADF, TypeError => e
        retry
      end
      return unless readable || errors

      errors.each do |client|
        begin
          client.shutdown unless RUBY_PLATFORM =~ /java/
          client.close
        rescue
        end
        clients.delete(client)
      end

      available_clients, available_servers = readable.partition { |s| s.class == TCPSocket }

      available_servers.each do |server|
        available_clients << server.accept
      end

      closed, open = available_clients.partition do |client|
        begin
          client.eof?
        rescue IOError
          true
        end
      end
      closed.each { |client| available_clients.delete(client) }

      if client = open.shift
        server = lookup_server(client)
        return server, client
      else
        nil
      end
    end

    # Remove the server from the manager.
    #
    # @example Remove the server from the manager.
    #   manager.remove(server)
    #
    # @param [ Server ] server The server to remove.
    #
    # @since 2.0.0
    def remove(server)
      close_clients!(server)
      servers.delete_if{ |s| s.seed == server.seed }
    end

    # Shutdown the manager.
    #
    # @example Shutdown the manager.
    #   manager.shutdown
    #
    # @return [ true ] Always true.
    #
    # @since 2.0.0
    def shutdown
      clients.each do |client|
        begin
          client.shutdown unless RUBY_PLATFORM =~ /java/
          client.close
        rescue; end
      end
      @shutdown = true
    end

    private

    def lookup_server(client)
      port = client.addr(false)[1]
      servers.find do |server|
        server.to_io && server.to_io.addr[1] == port
      end
    end
  end

  # Adds let context helpers to specs.
  #
  # @since 2.0.0
  module Helpers

    # Define the lets when included.
    #
    # @param [ Class ] context The RSpec context.
    #
    # @since 2.0.0
    def self.included(context)
      context.let(:simulator_seeds) { @simulator_seeds }
      context.let(:simulator) { @simulator }
    end
  end

  class << self

    # Configure the cluster simulator to instantiate on specs with the cluster
    # metdata flagged as true.
    #
    # @example Configure the cluster simulator.
    #   ClusterSimulator.configure(config)
    #
    # @param [ RSpec::Configuration ] config The RSpec config.
    #
    # @since 2.0.0
    def configure(config)
      config.before(:all, simulator: 'cluster') do |example|
        @simulator_seeds = [ '127.0.0.1:27018', '127.0.0.1:27019', '127.0.0.1:27020']
        @simulator = ClusterSimulator.new(@simulator_seeds).start
      end

      config.after(:each, simulator: 'cluster') do
        @simulator.servers.each(&:restart)
      end

      config.after(:all, simulator: 'cluster') do
        @simulator.stop
      end
    end
  end
end

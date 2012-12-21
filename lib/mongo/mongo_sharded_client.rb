# encoding: UTF-8

# --
# Copyright (C) 2008-2012 10gen Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ++

module Mongo

  # Instantiates and manages connections to a MongoDB sharded cluster for high availability.
  class MongoShardedClient < MongoReplicaSetClient
    include ThreadLocalVariableManager

    SHARDED_CLUSTER_OPTS = [:refresh_mode, :refresh_interval, :tag_sets, :read]

    attr_reader :seeds, :refresh_interval, :refresh_mode,
                :refresh_version, :manager

    def initialize(*args)
      opts = args.last.is_a?(Hash) ? args.pop : {}

      nodes = args.flatten

      if nodes.empty? and ENV.has_key?('MONGODB_URI')
        parser = URIParser.new ENV['MONGODB_URI']
        if parser.direct?
          raise MongoArgumentError, "Mongo::MongoShardedClient.new called with no arguments, but ENV['MONGODB_URI'] implies a direct connection."
        end
        opts = parser.connection_options.merge! opts
        nodes = [parser.nodes]
      end

      unless nodes.length > 0
        raise MongoArgumentError, "A MongoShardedClient requires at least one seed node."
      end

      @seeds = nodes.map do |host_port|
        host, port = host_port.split(":")
        [ host, port.to_i ]
      end

      # TODO: add a method for replacing this list of node.
      @seeds.freeze

      # Refresh
      @last_refresh = Time.now
      @refresh_version = 0

      # No connection manager by default.
      @manager = nil
      @old_managers = []

      # Lock for request ids.
      @id_lock = Mutex.new

      @pool_mutex = Mutex.new
      @connected = false

      @safe_mutex_lock = Mutex.new
      @safe_mutexes = Hash.new {|hash, key| hash[key] = Mutex.new}

      @connect_mutex = Mutex.new
      @refresh_mutex = Mutex.new

      @mongos        = true

      check_opts(opts)
      setup(opts)
    end

    def valid_opts
      GENERIC_OPTS + SHARDED_CLUSTER_OPTS
    end

    def inspect
      "<Mongo::MongoShardedClient:0x#{self.object_id.to_s(16)} @seeds=#{@seeds.inspect} " +
          "@connected=#{@connected}>"
    end

    # Initiate a connection to the sharded cluster.
    def connect(force = !@connected)
      return unless force
      log(:info, "Connecting...")
      @connect_mutex.synchronize do
        discovered_seeds = @manager ? @manager.seeds : []
        @old_managers << @manager if @manager
        @manager = ShardingPoolManager.new(self, discovered_seeds | @seeds)

        thread_local[:managers][self] = @manager

        @manager.connect
        @refresh_version += 1
        @last_refresh = Time.now
        @connected = true
      end
    end

    # Force a hard refresh of this connection's view
    # of the sharded cluster.
    #
    # @return [Boolean] +true+ if hard refresh
    #   occurred. +false+ is returned when unable
    #   to get the refresh lock.
    def hard_refresh!
      log(:info, "Initiating hard refresh...")
      connect(true)
      return true
    end

    def connected?
      @connected && @manager.primary_pool
    end

    # Returns +true+ if it's okay to read from a secondary node.
    # Since this is a sharded cluster, this must always be false.
    #
    # This method exist primarily so that Cursor objects will
    # generate query messages with a slaveOkay value of +true+.
    #
    # @return [Boolean] +true+
    def slave_ok?
      false
    end

    def checkout(&block)
      2.times do
        if connected?
          sync_refresh
        else
          connect
        end

        begin
          socket = block.call
        rescue => ex
          checkin(socket) if socket
          raise ex
        end

        if socket
          return socket
        else
          @connected = false
          #raise ConnectionFailure.new("Could not checkout a socket.")
        end
      end
    end
  end
end

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

require 'mongo/cluster/topology'

module Mongo

  # Represents a group of servers on the server side, either as a single server, a
  # replica set, or a single or multiple mongos.
  #
  # @since 2.0.0
  class Cluster
    extend Forwardable
    include Event::Subscriber
    include Loggable

    # Constant for the replica set name configuration option.
    #
    # @since 2.0.0
    REPLICA_SET_NAME = :replica_set.freeze

    # @return [ Mongo::Client ] The cluster's client.
    attr_reader :client
    # @return [ Array<String> ] The provided seed addresses.
    attr_reader :addresses
    # @return [ Hash ] The options hash.
    attr_reader :options
    # @return [ Object ] The cluster topology.
    attr_reader :topology

    def_delegators :topology, :sharded?

    # Determine if this cluster of servers is equal to another object. Checks the
    # servers currently in the cluster, not what was configured.
    #
    # @example Is the cluster equal to the object?
    #   cluster == other
    #
    # @param [ Object ] other The object to compare to.
    #
    # @return [ true, false ] If the objects are equal.
    #
    # @since 2.0.0
    def ==(other)
      return false unless other.is_a?(Cluster)
      addresses == other.addresses
    end

    # Add a server to the cluster with the provided address. Useful in
    # auto-discovery of new servers when an existing server executes an ismaster
    # and potentially non-configured servers were included.
    #
    # @example Add the server for the address to the cluster.
    #   cluster.add('127.0.0.1:27018')
    #
    # @param [ String ] address The address of the server to add.
    #
    # @return [ Server ] The newly added server, if not present already.
    #
    # @since 2.0.0
    def add(address)
      unless addresses.include?(address)
        log(:debug, 'MONGODB', [ "Adding #{address} to the cluster." ])
        addresses.push(address)
        server = Server.new(address, options)
        @servers.push(server)
        server
      end
    end

    # Instantiate the new cluster.
    #
    # @example Instantiate the cluster.
    #   Mongo::Cluster.new(["127.0.0.1:27017"])
    #
    # @param [ Array<String> ] addresses The addresses of the configured servers.
    # @param [ Hash ] options The options.
    #
    # @since 2.0.0
    def initialize(client, addresses, options = {})
      @client = client
      @addresses = addresses
      @options = options.freeze
      @topology = Topology.get(options)
      @servers = addresses.map do |address|
        Server.new(address, options)
      end
      subscribe_to(Event::SERVER_ADDED, Event::ServerAdded.new(self))
      subscribe_to(Event::SERVER_REMOVED, Event::ServerRemoved.new(self))
    end

    # Get the next primary server we can send an operation to.
    #
    # @example Get the next primary server.
    #   cluster.next_primary
    #
    # @return [ Mongo::Server ] A primary server.
    #
    # @since 2.0.0
    def next_primary
      client.server_preference.primary(servers).first
    end

    # Removed the server from the cluster for the provided address, if it
    # exists.
    #
    # @example Remove the server from the cluster.
    #   server.remove('127.0.0.1:27017')
    #
    # @param [ String ] address The host/port or socket address.
    #
    # @since 2.0.0
    def remove(address)
      removed_servers = @servers.reject!{ |server| server.address.seed == address }
      removed_servers.each{ |server| server.disconnect! } if removed_servers
      addresses.reject!{ |addr| addr == address }
    end

    # Get the replica set name configured for this cluster.
    #
    # @example Get the replica set name.
    #   cluster.replica_set_name
    #
    # @return [ String ] The name of the configured replica set.
    #
    # @since 2.0.0
    def replica_set_name
      options[REPLICA_SET_NAME]
    end

    # Get a list of server candidates from the cluster that can have operations
    # executed on them.
    #
    # @example Get the server candidates for an operation.
    #   cluster.servers
    #
    # @return [ Array<Server> ] The candidate servers.
    #
    # @since 2.0.0
    def servers
      topology.servers(@servers, replica_set_name)
    end
  end
end

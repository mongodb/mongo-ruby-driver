# Copyright (C) 2014-2015 MongoDB, Inc.
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

    # @return [ Array<String> ] The provided seed addresses.
    attr_reader :addresses

    # @return [ Hash ] The options hash.
    attr_reader :options

    # @return [ Mongo::ServerSelector ] The read preference.
    attr_reader :read_preference

    # @return [ Object ] The cluster topology.
    attr_reader :topology

    def_delegators :topology, :replica_set?, :replica_set_name, :sharded?, :standalone?, :unknown?

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
      addresses == other.addresses && options == other.options
    end

    # Add a server to the cluster with the provided address. Useful in
    # auto-discovery of new servers when an existing server executes an ismaster
    # and potentially non-configured servers were included.
    #
    # @example Add the server for the address to the cluster.
    #   cluster.add('127.0.0.1:27018')
    #
    # @param [ String ] host The address of the server to add.
    #
    # @return [ Server ] The newly added server, if not present already.
    #
    # @since 2.0.0
    def add(host)
      address = Address.new(host)
      unless addresses.include?(address)
        log_debug([ "Adding #{address.to_s} to the cluster." ])
        addresses.push(address)
        server = Server.new(address, event_listeners, options)
        @servers.push(server)
        server
      end
    end

    # Instantiate the new cluster.
    #
    # @example Instantiate the cluster.
    #   Mongo::Cluster.new(["127.0.0.1:27017"])
    #
    # @param [ Array<String> ] seeds The addresses of the configured servers.
    # @param [ Hash ] options The options.
    #
    # @since 2.0.0
    def initialize(seeds, read_preference, event_listeners, options = {})
      @addresses = []
      @servers = []
      @event_listeners = event_listeners
      @read_preference = read_preference
      @options = options.freeze
      @topology = Topology.initial(seeds, options)

      subscribe_to(Event::SERVER_ADDED, Event::ServerAdded.new(self))
      subscribe_to(Event::SERVER_REMOVED, Event::ServerRemoved.new(self))
      subscribe_to(Event::PRIMARY_ELECTED, Event::PrimaryElected.new(self))

      seeds.each{ |seed| add(seed) }
    end

    # Get the nicer formatted string for use in inspection.
    #
    # @example Inspect the cluster.
    #   cluster.inspect
    #
    # @return [ String ] The cluster inspection.
    #
    # @since 2.0.0
    def inspect
      "#<Mongo::Cluster:0x#{object_id} servers=#{servers} topology=#{topology.display_name}>"
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
      ServerSelector.get(mode: :primary).select_server(self)
    end

    # Elect a primary server from the description that has just changed to a
    # primary.
    #
    # @example Elect a primary server.
    #   cluster.elect_primary!(description)
    #
    # @param [ Server::Description ] description The newly elected primary.
    #
    # @return [ Topology ] The cluster topology.
    #
    # @since 2.0.0
    def elect_primary!(description)
      @topology = topology.elect_primary(description, @servers)
    end

    # Removed the server from the cluster for the provided address, if it
    # exists.
    #
    # @example Remove the server from the cluster.
    #   server.remove('127.0.0.1:27017')
    #
    # @param [ String ] host The host/port or socket address.
    #
    # @since 2.0.0
    def remove(host)
      log_debug([ "#{host} being removed from the cluster." ])
      address = Address.new(host)
      removed_servers = @servers.reject!{ |server| server.address == address }
      removed_servers.each{ |server| server.disconnect! } if removed_servers
      addresses.reject!{ |addr| addr == address }
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
      topology.servers(@servers)
    end
  end
end

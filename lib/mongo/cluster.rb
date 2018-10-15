# Copyright (C) 2014-2018 MongoDB, Inc.
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
require 'mongo/cluster/reapers/socket_reaper'
require 'mongo/cluster/reapers/cursor_reaper'
require 'mongo/cluster/periodic_executor'

module Mongo

  # Represents a group of servers on the server side, either as a
  # single server, a replica set, or a single or multiple mongos.
  #
  # @since 2.0.0
  class Cluster
    extend Forwardable
    include Monitoring::Publishable
    include Event::Subscriber
    include Loggable

    # The default number of mongos read retries.
    #
    # @since 2.1.1
    MAX_READ_RETRIES = 1

    # The default number of mongos write retries.
    #
    # @since 2.4.2
    MAX_WRITE_RETRIES = 1

    # The default mongos read retry interval, in seconds.
    #
    # @since 2.1.1
    READ_RETRY_INTERVAL = 5

    # How often an idle primary writes a no-op to the oplog.
    #
    # @since 2.4.0
    IDLE_WRITE_PERIOD_SECONDS = 10

    # The cluster time key in responses from mongos servers.
    #
    # @since 2.5.0
    CLUSTER_TIME = 'clusterTime'.freeze

    # Instantiate the new cluster.
    #
    # @api private
    #
    # @example Instantiate the cluster.
    #   Mongo::Cluster.new(["127.0.0.1:27017"], monitoring)
    #
    # @note Cluster should never be directly instantiated outside of a Client.
    #
    # @note When connecting to a mongodb+srv:// URI, the client expands such a
    #   URI into a list of servers and passes that list to the Cluster
    #   constructor. When connecting to a standalone mongod, the Cluster
    #   constructor receives the corresponding address as an array of one string.
    #
    # @param [ Array<String> ] seeds The addresses of the configured servers
    # @param [ Monitoring ] monitoring The monitoring.
    # @param [ Hash ] options Options. Client constructor forwards its
    #   options to Cluster constructor, although Cluster recognizes
    #   only a subset of the options recognized by Client.
    #
    # @since 2.0.0
    def initialize(seeds, monitoring, options = Options::Redacted.new)
      @servers = []
      @monitoring = monitoring
      @event_listeners = Event::Listeners.new
      @options = options.freeze
      @app_metadata = Server::AppMetadata.new(@options)
      @update_lock = Mutex.new
      @pool_lock = Mutex.new
      @cluster_time = nil
      @cluster_time_lock = Mutex.new
      @topology = Topology.initial(self, monitoring, options)
      Session::SessionPool.create(self)

      # The opening topology is always unknown with no servers.
      # https://github.com/mongodb/specifications/pull/388
      opening_topology = Topology::Unknown.new(options, monitoring, self)

      publish_sdam_event(
        Monitoring::TOPOLOGY_OPENING,
        Monitoring::Event::TopologyOpening.new(opening_topology)
      )

      subscribe_to(Event::STANDALONE_DISCOVERED, Event::StandaloneDiscovered.new(self))
      subscribe_to(Event::DESCRIPTION_CHANGED, Event::DescriptionChanged.new(self))
      subscribe_to(Event::MEMBER_DISCOVERED, Event::MemberDiscovered.new(self))

      @seeds = seeds
      seeds.each{ |seed| add(seed) }

      publish_sdam_event(
        Monitoring::TOPOLOGY_CHANGED,
        Monitoring::Event::TopologyChanged.new(opening_topology, @topology)
      ) if seeds.size > 1

      @cursor_reaper = CursorReaper.new
      @socket_reaper = SocketReaper.new(self)
      @periodic_executor = PeriodicExecutor.new(@cursor_reaper, @socket_reaper)
      @periodic_executor.run!

      ObjectSpace.define_finalizer(self, self.class.finalize(pools, @periodic_executor, @session_pool))

      @connecting = false
      @connected = true
    end

    # Create a cluster for the provided client, for use when we don't want the
    # client's original cluster instance to be the same.
    #
    # @api private
    #
    # @example Create a cluster for the client.
    #   Cluster.create(client)
    #
    # @param [ Client ] client The client to create on.
    #
    # @return [ Cluster ] The cluster.
    #
    # @since 2.0.0
    def self.create(client)
      cluster = Cluster.new(
        client.cluster.addresses.map(&:to_s),
        Monitoring.new,
        client.options
      )
      client.instance_variable_set(:@cluster, cluster)
    end

    # @return [ Hash ] The options hash.
    attr_reader :options

    # @return [ Monitoring ] monitoring The monitoring.
    attr_reader :monitoring

    # @return [ Object ] The cluster topology.
    attr_reader :topology

    # @return [ Mongo::Server::AppMetadata ] The application metadata, used for connection
    #   handshakes.
    #
    # @since 2.4.0
    attr_reader :app_metadata

    # @return [ BSON::Document ] The latest cluster time seen.
    #
    # @since 2.5.0
    attr_reader :cluster_time

    # @return [ Array<String> ] The addresses of seed servers. Contains
    #   addresses that were given to Cluster when it was instantiated, not
    #   current addresses that the cluster is using as a result of SDAM.
    #
    # @since 2.7.0
    # @api private
    attr_reader :seeds

    # @private
    #
    # @since 2.5.1
    attr_reader :session_pool

    def_delegators :topology, :replica_set?, :replica_set_name, :sharded?,
                   :single?, :unknown?
    def_delegators :@cursor_reaper, :register_cursor, :schedule_kill_cursor, :unregister_cursor

    # Get the maximum number of times the cluster can retry a read operation on
    # a mongos.
    #
    # @example Get the max read retries.
    #   cluster.max_read_retries
    #
    # @return [ Integer ] The maximum retries.
    #
    # @since 2.1.1
    def max_read_retries
      options[:max_read_retries] || MAX_READ_RETRIES
    end

    # Get the interval, in seconds, in which a mongos read operation is
    # retried.
    #
    # @example Get the read retry interval.
    #   cluster.read_retry_interval
    #
    # @return [ Float ] The interval.
    #
    # @since 2.1.1
    def read_retry_interval
      options[:read_retry_interval] || READ_RETRY_INTERVAL
    end

    # Whether the cluster object is connected to its cluster.
    #
    # @return [ true|false ] Whether the cluster is connected.
    #
    # @api private
    # @since 2.7.0
    def connected?
      !!@connected
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
      topology.servers(servers_list.compact).compact
    end

    # The addresses in the cluster.
    #
    # @example Get the addresses in the cluster.
    #   cluster.addresses
    #
    # @return [ Array<Mongo::Address> ] The addresses.
    #
    # @since 2.0.6
    def addresses
      servers_list.map(&:address).dup
    end

    # The logical session timeout value in minutes.
    #
    # @example Get the logical session timeout in minutes.
    #   cluster.logical_session_timeout
    #
    # @return [ Integer, nil ] The logical session timeout.
    #
    # @since 2.5.0
    def logical_session_timeout
      servers.inject(nil) do |min, server|
        break unless timeout = server.logical_session_timeout
        [timeout, (min || timeout)].min
      end
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
      "#<Mongo::Cluster:0x#{object_id} servers=#{servers} topology=#{topology.summary}>"
    end

    # @api experimental
    def summary
      "#<Cluster " +
      "topology=#{topology.summary} "+
      "servers=[#{servers.map(&:summary).join(',')}]>"
    end

    # Finalize the cluster for garbage collection. Disconnects all the scoped
    # connection pools.
    #
    # @example Finalize the cluster.
    #   Cluster.finalize(pools)
    #
    # @param [ Hash<Address, Server::ConnectionPool> ] pools The connection pools.
    # @param [ PeriodicExecutor ] periodic_executor The periodic executor.
    # @param [ SessionPool ] session_pool The session pool.
    #
    # @return [ Proc ] The Finalizer.
    #
    # @since 2.2.0
    def self.finalize(pools, periodic_executor, session_pool)
      proc do
        session_pool.end_sessions
        periodic_executor.stop!
        pools.values.each do |pool|
          pool.disconnect!
        end
      end
    end

    # Disconnect all servers.
    #
    # @note Applications should call Client#close to disconnect from
    # the cluster rather than calling this method. This method is for
    # internal driver use only.
    #
    # @example Disconnect the cluster's servers.
    #   cluster.disconnect!
    #
    # @return [ true ] Always true.
    #
    # @since 2.1.0
    def disconnect!
      unless @connecting || @connected
        return true
      end
      @periodic_executor.stop!
      @servers.each do |server|
        server.disconnect!
        publish_sdam_event(
          Monitoring::SERVER_CLOSED,
          Monitoring::Event::ServerClosed.new(server.address, topology)
        )
      end
      publish_sdam_event(
        Monitoring::TOPOLOGY_CLOSED,
        Monitoring::Event::TopologyClosed.new(topology)
      )
      @connecting = @connected = false
      true
    end

    # Reconnect all servers.
    #
    # @example Reconnect the cluster's servers.
    #   cluster.reconnect!
    #
    # @return [ true ] Always true.
    #
    # @since 2.1.0
    # @deprecated Use Client#reconnect to reconnect to the cluster instead of
    #   calling this method. This method does not send SDAM events.
    def reconnect!
      @connecting = true
      scan!
      servers.each do |server|
        server.reconnect!
      end
      @periodic_executor.restart!
      @connecting = false
      @connected = true
    end

    # Force a scan of all known servers in the cluster.
    #
    # @example Force a full cluster scan.
    #   cluster.scan!
    #
    # @note This operation is done synchronously. If servers in the cluster are
    #   down or slow to respond this can potentially be a slow operation.
    #
    # @return [ true ] Always true.
    #
    # @since 2.0.0
    def scan!
      servers_list.each{ |server| server.scan! } and true
    end

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

    # Determine if the cluster would select a readable server for the
    # provided read preference.
    #
    # @example Is a readable server present?
    #   topology.has_readable_server?(server_selector)
    #
    # @param [ ServerSelector ] server_selector The server
    #   selector.
    #
    # @return [ true, false ] If a readable server is present.
    #
    # @since 2.4.0
    def has_readable_server?(server_selector = nil)
      topology.has_readable_server?(self, server_selector)
    end

    # Determine if the cluster would select a writable server.
    #
    # @example Is a writable server present?
    #   topology.has_writable_server?
    #
    # @return [ true, false ] If a writable server is present.
    #
    # @since 2.4.0
    def has_writable_server?
      topology.has_writable_server?(self)
    end

    # Get the next primary server we can send an operation to.
    #
    # @example Get the next primary server.
    #   cluster.next_primary
    #
    # @param [ true, false ] ping Whether to ping the server before selection. Deprecated,
    #   not necessary with the implementation of the Server Selection specification.
    #
    #
    # @return [ Mongo::Server ] A primary server.
    #
    # @since 2.0.0
    def next_primary(ping = true)
      @primary_selector ||= ServerSelector.get(ServerSelector::PRIMARY)
      @primary_selector.select_server(self)
    end

    # Get the scoped connection pool for the server.
    #
    # @example Get the connection pool.
    #   cluster.pool(server)
    #
    # @param [ Server ] server The server.
    #
    # @return [ Server::ConnectionPool ] The connection pool.
    #
    # @since 2.2.0
    def pool(server)
      @pool_lock.synchronize do
        pools[server.address] ||= Server::ConnectionPool.get(server)
      end
    end

    # Update the max cluster time seen in a response.
    #
    # @example Update the cluster time.
    #   cluster.update_cluster_time(result)
    #
    # @param [ Operation::Result ] result The operation result containing the cluster time.
    #
    # @return [ Object ] The cluster time.
    #
    # @since 2.5.0
    def update_cluster_time(result)
      if cluster_time_doc = result.cluster_time
        @cluster_time_lock.synchronize do
          if @cluster_time.nil?
            @cluster_time = cluster_time_doc
          elsif cluster_time_doc[CLUSTER_TIME] > @cluster_time[CLUSTER_TIME]
            @cluster_time = cluster_time_doc
          end
        end
      end
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
      address = Address.new(host, options)
      if !addresses.include?(address)
        if addition_allowed?(address)
          server = Server.new(address, self, @monitoring, event_listeners, options.merge(
            monitor: false))
          @update_lock.synchronize { @servers.push(server) }
          server.start_monitoring
          server
        end
      end
    end

    # Remove the server from the cluster for the provided address, if it
    # exists.
    #
    # @example Remove the server from the cluster.
    #   server.remove('127.0.0.1:27017')
    #
    # @param [ String ] host The host/port or socket address.
    #
    # @return [ true|false ] Whether any servers were removed.
    #
    # @since 2.0.0, return value added in 2.7.0
    def remove(host)
      address = Address.new(host)
      removed_servers = @servers.select { |s| s.address == address }
      @update_lock.synchronize { @servers = @servers - removed_servers }
      removed_servers.each do |server|
        server.disconnect!
      end
      publish_sdam_event(
        Monitoring::SERVER_CLOSED,
        Monitoring::Event::ServerClosed.new(address, topology)
      )
      removed_servers.any?
    end

    # Add hosts in a description to the cluster.
    #
    # @example Add hosts in a description to the cluster.
    #   cluster.add_hosts(description)
    #
    # @param [ Mongo::Server::Description ] description The description.
    #
    # @since 2.0.6
    def add_hosts(description)
      if topology.add_hosts?(description, servers_list)
        description.servers.each { |s| add(s) }
      end
    end

    # Remove hosts in a description from the cluster.
    #
    # @example Remove hosts in a description from the cluster.
    #   cluster.remove_hosts(description)
    #
    # @param [ Mongo::Server::Description ] description The description.
    #
    # @since 2.0.6
    def remove_hosts(description)
      if topology.remove_hosts?(description)
        servers_list.each do |s|
          remove(s.address.to_s) if topology.remove_server?(description, s)
        end
      end
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
      # Update descriptions - temporary hack until
      # https://jira.mongodb.org/browse/RUBY-1509 is implemented.
      # There are multiple placet that can update descriptions, these should
      # be DRYed to a single one.
      servers = servers_list
      servers.each do |server|
        if server.address == description.address
          server.update_description(description)
        end
      end

      new_topology = nil
      if topology.unknown?
        new_topology = if description.mongos?
          Topology::Sharded.new(topology.options, topology.monitoring, self)
        else
          initialize_replica_set(description, servers)
        end
      elsif topology.replica_set?
        if description.replica_set_name == replica_set_name
          if detect_stale_primary!(description)
            # Since detect_stale_primary! can mutate description,
            # we need another pass of updating descriptions on our servers.
            # https://jira.mongodb.org/browse/RUBY-1509
            servers.each do |server|
              if server.address == description.address
                server.update_description(description)
              end
            end
          else
            # If we had another server marked as primary, mark that one
            # unknown.
            servers.each do |server|
              if server.primary? && server.address != description.address
                server.description.unknown!
              end
            end

            # This mutates the old topology.
            # Instead of this the old topology should be left untouched
            # and the new values should only be given to the new topology.
            # But since there is some logic in these methods,
            # this will be addressed by https://jira.mongodb.org/browse/RUBY-1511
            topology.update_max_election_id(description)
            topology.update_max_set_version(description)

            cls = if servers.any?(&:primary?)
              Topology::ReplicaSetWithPrimary
            else
              Topology::ReplicaSetNoPrimary
            end
            new_topology = cls.new(topology.options,
              topology.monitoring,
              self,
              topology.max_election_id,
              topology.max_set_version)
          end
        else
          log_warn(
            "Server #{description.address.to_s} has incorrect replica set name: " +
            "'#{description.replica_set_name}'. The current replica set name is '#{topology.replica_set_name}'."
          )
        end
      end

      if new_topology
        update_topology(new_topology)
        # Even though the topology class selection above already attempts
        # to figure out if the topology has a primary, in some cases
        # we already are in a replica set topology and an additional
        # primary check must be performed here.
        if topology.replica_set?
          check_if_has_primary
        end
      end
    end

    # Notify the cluster that a standalone server was discovered so that the
    # topology can be updated accordingly.
    #
    # @example Notify the cluster that a standalone server was discovered.
    #   cluster.standalone_discovered
    #
    # @return [ Topology ] The cluster topology.
    #
    # @since 2.0.6
    def standalone_discovered
      if topology.unknown?
        if seeds.length == 1
          update_topology(
            Topology::Single.new(topology.options, topology.monitoring, self))
        end
      end
      topology
    end

    # Handles a change in server description.
    #
    # @param [ Server::Description ] previous_description Previous server description.
    # @param [ Server::Description ] updated_description The changed description.
    #
    # @api private
    def server_description_changed(previous_description, updated_description)
      # https://jira.mongodb.org/browse/RUBY-1509
      servers_list.each do |server|
        if server.address == updated_description.address
          server.update_description(updated_description)
        end
      end

      publish_sdam_event(
        Monitoring::SERVER_DESCRIPTION_CHANGED,
        Monitoring::Event::ServerDescriptionChanged.new(
          updated_description.address,
          topology,
          previous_description,
          updated_description,
        )
      )

      add_hosts(updated_description)
      remove_hosts(updated_description)

      if updated_description.ghost? && !topology.is_a?(Topology::Sharded)
        # https://jira.mongodb.org/browse/RUBY-1509
        servers.each do |server|
          if server.address == updated_description.address
            server.update_description(updated_description)
          end
        end
      end

      if topology.is_a?(::Mongo::Cluster::Topology::Unknown) &&
        updated_description.replica_set_name &&
        updated_description.replica_set_name != ''
      then
        transition_to_replica_set(updated_description)
=begin pending further refactoring
      elsif topology.is_a?(Cluster::Topology::ReplicaSetWithPrimary) &&
        (updated_description.unknown? ||
          updated_description.standalone? ||
          updated_description.mongos? ||
          updated_description.ghost?)
      then
        # here the unknown server is already removed from the topology
        check_if_has_primary
=end
      end

      # This check may be invoked in more cases than is necessary per
      # the spec, but currently it is hard to know exactly when it should
      # be invoked and the commented out condition above does not catch
      # all cases. In any event check_if_has_primary is harmless if
      # the topology does not transition.
      if topology.replica_set?
        check_if_has_primary
      end
    end

    # @api private
    def member_discovered
      if topology.unknown? || topology.single?
        publish_sdam_event(Monitoring::TOPOLOGY_CHANGED,
          Monitoring::Event::TopologyChanged.new(topology, topology))
      end
    end

    private

    # Checks if the cluster has a primary, and if not, transitions the topology
    # to ReplicaSetNoPrimary. Topology must be ReplicaSetWithPrimary when
    # invoking this method.
    #
    # @api private
    def check_if_has_primary
      unless topology.replica_set?
        raise ArgumentError, 'check_if_has_primary should only be called when topology is replica set'
      end

      primary = servers.detect do |server|
        # A primary with the wrong set name is not a primary
        server.primary? && server.description.replica_set_name == topology.replica_set_name
      end
      unless primary
        update_topology(Topology::ReplicaSetNoPrimary.new(
          topology.options, topology.monitoring, self,
          topology.max_election_id, topology.max_set_version))
      end
    end

    # Transitions topology from unknown to one of the two replica set
    # topologies, depending on whether the updated description came from
    # a primary. Topology must be Unknown when invoking this method.
    #
    # @param [ Server::Description ] updated_description The changed description.
    #
    # @api private
    def transition_to_replica_set(updated_description)
      new_cls = if updated_description.primary?
        ::Mongo::Cluster::Topology::ReplicaSetWithPrimary
      else
        ::Mongo::Cluster::Topology::ReplicaSetNoPrimary
      end
      update_topology(new_cls.new(
        topology.options.merge(
          replica_set: updated_description.replica_set_name,
        ), topology.monitoring, self))
    end

    # Creates a replica set topology, either having the primary or
    # not, based on description and servers provided.
    # May mutate servers' descriptions.
    #
    # Description must be of a server in the replica set topology, and
    # is used to obtain the replica set name among other things.
    def initialize_replica_set(description, servers)
      servers.each do |server|
        if server.standalone? && server.address != description.address
          server.description.unknown!
        end
      end
      cls = if servers.any?(&:primary?)
        Topology::ReplicaSetWithPrimary
      else
        Topology::ReplicaSetNoPrimary
      end
      cls.new(topology.options.merge(:replica_set => description.replica_set_name),
        topology.monitoring, self)
    end

    # Checks whether description is for a stale primary, and if so,
    # changes the description to be unknown.
    def detect_stale_primary!(description)
      if description.election_id && description.set_version
        if topology.max_set_version && topology.max_election_id &&
            (description.set_version < topology.max_set_version ||
                (description.set_version == topology.max_set_version &&
                    description.election_id < topology.max_election_id))
          description.unknown!
        end
      end
    end

    def update_topology(new_topology)
      old_topology = topology
      @topology = new_topology
      publish_sdam_event(
        Monitoring::TOPOLOGY_CHANGED,
        Monitoring::Event::TopologyChanged.new(old_topology, topology)
      )
    end

    def get_session(client, options = {})
      return options[:session].validate!(self) if options[:session]
      if sessions_supported?
        Session.new(@session_pool.checkout, client, { implicit: true }.merge(options))
      end
    end

    def with_session(client, options = {})
      session = get_session(client, options)
      yield(session)
    ensure
      session.end_session if (session && session.implicit?)
    end

    def sessions_supported?
      if servers.empty? && !topology.single?
        ServerSelector.get(mode: :primary_preferred).select_server(self)
      end
      !!logical_session_timeout
    rescue Error::NoServerAvailable
    end

    def addition_allowed?(address)
      if @topology.single?
        [address.seed] == @seeds
      else
        true
      end
    end

    def pools
      @pools ||= {}
    end

    def servers_list
      @update_lock.synchronize { @servers.dup }
    end
  end
end

# Copyright (C) 2014-2017 MongoDB, Inc.
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
require 'mongo/cluster/app_metadata'

module Mongo

  # Represents a group of servers on the server side, either as a single server, a
  # replica set, or a single or multiple mongos.
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

    # @return [ Hash ] The options hash.
    attr_reader :options

    # @return [ Monitoring ] monitoring The monitoring.
    attr_reader :monitoring

    # @return [ Object ] The cluster topology.
    attr_reader :topology

    # @return [ Mongo::Cluster::AppMetadata ] The application metadata, used for connection
    #   handshakes.
    #
    # @since 2.4.0
    attr_reader :app_metadata

    # @return [ BSON::Document ] The latest cluster time seen.
    #
    # @since 2.5.0
    attr_reader :cluster_time

    # @private
    #
    # @since 2.5.1
    attr_reader :session_pool

    def_delegators :topology, :replica_set?, :replica_set_name, :sharded?,
                   :single?, :unknown?, :member_discovered
    def_delegators :@cursor_reaper, :register_cursor, :schedule_kill_cursor, :unregister_cursor

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
      address = Address.new(host, options)
      if !addresses.include?(address)
        if addition_allowed?(address)
          @update_lock.synchronize { @addresses.push(address) }
          server = Server.new(address, self, @monitoring, event_listeners, options)
          @update_lock.synchronize { @servers.push(server) }
          server
        end
      end
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
      @addresses = []
      @servers = []
      @monitoring = monitoring
      @event_listeners = Event::Listeners.new
      @options = options.freeze
      @app_metadata = AppMetadata.new(self)
      @update_lock = Mutex.new
      @pool_lock = Mutex.new
      @cluster_time = nil
      @cluster_time_lock = Mutex.new
      p seeds, options
      @topology = Topology.initial(seeds, monitoring, options.merge(cluster: self))
      Session::SessionPool.create(self)

      publish_sdam_event(
        Monitoring::TOPOLOGY_OPENING,
        Monitoring::Event::TopologyOpening.new(@topology)
      )

      if @topology.is_a?(Topology::Unknown) && servers.length > 0 && options[:connect] != :direct
      end

      subscribe_to(Event::STANDALONE_DISCOVERED, Event::StandaloneDiscovered.new(self))
      subscribe_to(Event::DESCRIPTION_CHANGED, Event::DescriptionChanged.new(self))
      subscribe_to(Event::MEMBER_DISCOVERED, Event::MemberDiscovered.new(self))

      seeds.each do |seed|
        add(seed)
      end

      if addresses.size > 0
        # The spec wants us to emit a topology changed event going from
        # no servers to our seed list of servers, with unknown topology
        empty_topology = @topology.class.new(@topology.options, monitoring, [])
        publish_sdam_event(
          Monitoring::TOPOLOGY_CHANGED,
          Monitoring::Event::TopologyChanged.new(empty_topology, @topology)
        )

=begin
        publish_sdam_event(
          Monitoring::TOPOLOGY_CHANGED,
          Monitoring::Event::TopologyChanged.new(@topology, @topology)
        )
=end
      end

      @cursor_reaper = CursorReaper.new
      @socket_reaper = SocketReaper.new(self)
      @periodic_executor = PeriodicExecutor.new(@cursor_reaper, @socket_reaper)
      @periodic_executor.run!

      ObjectSpace.define_finalizer(self, self.class.finalize(pools, @periodic_executor, @session_pool))
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

    def inspect_verbose
      %Q~#<Mongo::Cluster:0x#{object_id}
  addresses=#{addresses.map(&:to_s)}
  topology=#{topology.display_name}
  servers=#{servers.map(&:inspect_verbose)}
>~
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
      @topology = topology.elect_primary(description, servers_list)
    end

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
      #@topology = topology.standalone_discovered
    end

    # Remove the server from the cluster for the provided address, if it
    # exists.
    #
    # @example Remove the server from the cluster.
    #   server.remove('127.0.0.1:27017')
    #
    # @param [ String ] host The host/port or socket address.
    #
    # @since 2.0.0
    def remove(host)
      address = Address.new(host)
      removed_servers = @servers.select { |s| s.address == address }
      @update_lock.synchronize { @servers = @servers - removed_servers }
      removed_servers.each{ |server| server.disconnect! } if removed_servers
      publish_sdam_event(
        Monitoring::SERVER_CLOSED,
        Monitoring::Event::ServerClosed.new(address, topology)
      )
      @update_lock.synchronize { @addresses.reject! { |addr| addr == address } }
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

    # Disconnect all servers.
    #
    # @example Disconnect the cluster's servers.
    #   cluster.disconnect!
    #
    # @return [ true ] Always true.
    #
    # @since 2.1.0
    def disconnect!
      @periodic_executor.stop!
      @servers.each { |server| server.disconnect! } and true
    end

    # Reconnect all servers.
    #
    # @example Reconnect the cluster's servers.
    #   cluster.reconnect!
    #
    # @return [ true ] Always true.
    #
    # @since 2.1.0
    def reconnect!
      scan!
      servers.each { |server| server.reconnect! }
      @periodic_executor.restart! and true
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
        client.instance_variable_get(:@monitoring).dup,
        client.options
      )
      client.instance_variable_set(:@cluster, cluster)
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
      addresses_list
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

    def server_description_changed(server, previous, updated)
      # A server description change does the following things:
      #
      # 1. The Server which returned the updated description
      #    may be removed from the Cluster object, if it is
      #    determined that it is not part of the cluster *requested
      #    by the client* (per the new description).
      #    This usually indicates a configuration problem because
      #    the client managed to connect to a mongod/mongos, yet some
      #    parameters specified by the client did not match the configuration
      #    of mongod/mongos. This could be, for example, because the client
      #    requested to connect to a replica set and the mongod is not part
      #    of a replica set. We want to surface server removal as a warning
      #    so that the user knows to adjust their client or cluster
      #    configuration.
      # 2. The description object on the server object is updated.
      #    Sounds circular but this update happens after
      #    server_description_changed is invoked, though the order should not
      #    be relied on - use `previous` and `updated` descriptions
      #    explicitly depending on which one is needed.
      # 3. New Server objects may be created and added to the Cluster
      #    object - this happens when the topology is a replica set.
      #    Any node can add servers to a cluster, however only the active
      #    primary can remove servers from a cluster.
      # 4. When the updated description is coming from the active primary
      #    in a replica set topology, we set the set of servers to exactly
      #    match whatever the primary returned. This can result in server
      #    removal as well as addition.
      #
      # Added servers always start out with Unknown description.

      #require 'byebug';byebug
      1

      # addresses method already dups, we don't have to here
      previous_addresses = addresses

      if topology.description_acceptable?(self, updated)
        # The cluster we connected to matches client requirements - good.
        # Add all new servers to our list of servers
        updated.servers.each do |host|
          # host here is a string like localhost:27017 or 127.0.0.1:27017
          add(host)
        end
      else
        # We established a connection to a cluster but the cluster does not
        # match client requirements. Warn the user to fix either the client
        # or the cluster configuration
        warn "Discarding #{server.address} due to a configuration mismatch - check replica set name"
        # Remove the server as we know it because updated.me may well be
        # different
        remove(server.address.to_s)
      end

      # description contains the address of the server but not the server object
      if updated.me_mismatch?
        warn "#{server.address} self-reports as #{updated.me} - removing #{server.address}"
        # We may still connect to the server we are removing here IF
        # we are connecting to a replica set, the replica set configuration
        # on the server matches what we requested, and the "correct" address
        # of the server is listed in `updated.servers`.
        # Note that the new connection may take a different network path,
        # for example if connecting to a hostname that maps to 127.0.0.1
        # in /etc/hosts but self-identifies with a non-loopback IP address.
        remove(server.address.to_s)
      end

      if topology.is_a?(Topology::ReplicaSet) && updated.primary?
        # Replace all of our servers with the list from the primary
        updated.servers.each do |host|
          unless addresses.any? { |address| address.to_s == host }
            add(host)
          end
        end

        addresses.each do |address|
          unless updated.servers.any? { |host| host == address.to_s }
            remove(address.to_s)
          end
        end
      end

      previous_topology = @topology

      new_topology = @topology.for_server_description(self, server, updated)

      # When transitioning from unknown topology to replica set,
      # specifications require sending a topology changed message
      # with the unknown topology type and new server set.
      # However, when transitioning from unknown to standalone
      # the expectation is to go from Unknown with no servers to
      # Single with one server.
      if previous_topology.is_a?(Topology::Unknown) && new_topology.is_a?(Topology::ReplicaSet)
      p [:XX,previous_addresses,addresses]
        if previous_addresses != addresses
          int_topology = Topology::Unknown.new(previous_topology.options,
            previous_topology.monitoring, addresses.map(&:to_s))
          p :unk_transition
          publish_sdam_event(
            Monitoring::TOPOLOGY_CHANGED,
            Monitoring::Event::TopologyChanged.new(previous_topology, int_topology)
          )
          previous_topology = int_topology
        end
      end

p :hm
p @topology
p previous_topology
      #if new_topology != @topology
      # Always publish a topology change event, because in the specification
      # a topology change event comes with the (updated) server set and
      # we are currently handling a server description change.
      # In the Ruby driver topology change event doesn't come with the
      # new or old server lists, and the topology class doesn't have any
      # knowledge of servers in any event, so that this event requires the
      # application to maintain cluster references in order to do something
      # useful as a response to topology change event when topology itself
      # remains the same.
      p :triggered, new_topology.class
        publish_sdam_event(
          Monitoring::TOPOLOGY_CHANGED,
          Monitoring::Event::TopologyChanged.new(previous_topology, new_topology)
        )
      #end

      @topology = new_topology
    end

    private

    def get_session(options = {})
      return options[:session].validate!(self) if options[:session]
      if sessions_supported?
        Session.new(@session_pool.checkout, self, { implicit: true }.merge(options))
      end
    end

    def with_session(options = {})
      session = get_session(options)
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

    def direct_connection?(address)
      address.seed == @topology.seed
    end

    def addition_allowed?(address)
      !@topology.single? || direct_connection?(address)
    end

    def pools
      @pools ||= {}
    end

    def servers_list
      @update_lock.synchronize { @servers.dup }
    end

    def addresses_list
      @update_lock.synchronize { @addresses.dup }
    end
  end
end

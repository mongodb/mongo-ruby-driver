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
    # @option options [ true, false ] :scan Whether to scan all seeds
    #   in constructor. The default in driver version 2.x is to do so;
    #   driver version 3.x will not scan seeds in constructor. Opt in to the
    #   new behavior by setting this option to false. *Note:* setting
    #   this option to nil enables scanning seeds in constructor in driver
    #   version 2.x. Driver version 3.x will recognize this option but
    #   will ignore it and will never scan seeds in the constructor.
    # @option options [ true, false ] :monitoring_io For internal driver
    #   use only. Set to false to prevent SDAM-related I/O from being
    #   done by this cluster or servers under it. Note: setting this option
    #   to false will make the cluster non-functional. It is intended for
    #   use in tests which manually invoke SDAM state transitions.
    #
    # @since 2.0.0
    def initialize(seeds, monitoring, options = Options::Redacted.new)
      if options[:monitoring_io] != false && !options[:server_selection_semaphore]
        raise ArgumentError, 'Need server selection semaphore'
      end

      @servers = []
      @monitoring = monitoring
      @event_listeners = Event::Listeners.new
      @options = options.freeze
      @app_metadata = Server::AppMetadata.new(@options)
      @update_lock = Mutex.new
      @sdam_flow_lock = Mutex.new
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

      subscribe_to(Event::DESCRIPTION_CHANGED, Event::DescriptionChanged.new(self))

      @seeds = seeds
      servers = seeds.map do |seed|
        # Server opening events must be sent after topology change events.
        # Therefore separate server addition, done here before topoolgy change
        # event is published, from starting to monitor the server which is
        # done later.
        add(seed, monitor: false)
      end

      if seeds.size >= 1
        # Recreate the topology to get the current server list into it
        @topology = topology.class.new(topology.options, topology.monitoring, self)
        publish_sdam_event(
          Monitoring::TOPOLOGY_CHANGED,
          Monitoring::Event::TopologyChanged.new(opening_topology, @topology)
        )
      end

      servers.each do |server|
        server.start_monitoring
      end

      if options[:monitoring_io] == false
        # Omit periodic executor construction, because without servers
        # no commands can be sent to the cluster and there shouldn't ever
        # be anything that needs to be cleaned up.
        #
        # Also omit legacy single round of SDAM on the main thread,
        # as it would race with tests that mock SDAM responses.
        return
      end

      @cursor_reaper = CursorReaper.new
      @socket_reaper = SocketReaper.new(self)
      @periodic_executor = PeriodicExecutor.new(@cursor_reaper, @socket_reaper)
      @periodic_executor.run!

      ObjectSpace.define_finalizer(self, self.class.finalize(pools, @periodic_executor, @session_pool))

      @connecting = false
      @connected = true

      if options[:scan] != false
        server_selection_timeout = options[:server_selection_timeout] || ServerSelector::SERVER_SELECTION_TIMEOUT
        # The server selection timeout can be very short especially in
        # tests, when the client waits for a synchronous scan before
        # starting server selection. Limiting the scan to server selection time
        # then aborts the scan before it can process even local servers.
        # Therefore, allow at least 3 seconds for the scan here.
        if server_selection_timeout < 3
          server_selection_timeout = 3
        end
        deadline = Time.now + server_selection_timeout
        # Wait for the first scan of each server to complete, for
        # backwards compatibility.
        # If any servers are discovered during this SDAM round we do NOT
        # wait for newly discovered servers to be queried.
        loop do
          servers = servers_list.dup
          if servers.all? { |server| server.last_scan_completed_at }
            break
          end
          if (time_remaining = deadline - Time.now) <= 0
            break
          end
          options[:server_selection_semaphore].wait(time_remaining)
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
        Monitoring.new,
        client.cluster_options,
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
    def_delegators :topology, :logical_session_timeout

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

    # @note This method is experimental and subject to change.
    #
    # @api experimental
    # @since 2.7.0
    def summary
      "#<Cluster " +
      "topology=#{topology.summary} "+
      "servers=[#{servers.map(&:summary).join(',')}]>"
    end

    # @api private
    attr_reader :server_selection_semaphore

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
        if server.connected?
          server.disconnect!
          publish_sdam_event(
            Monitoring::SERVER_CLOSED,
            Monitoring::Event::ServerClosed.new(server.address, topology)
          )
        end
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
    # If the sync parameter is true which is the default, the scan is
    # performed synchronously in the thread which called this method.
    # Each server in the cluster is checked sequentially. If there are
    # many servers in the cluster or they are slow to respond, this
    # can be a long running operation.
    #
    # If the sync parameter is false, this method instructs all server
    # monitor threads to perform an immediate scan and returns without
    # waiting for scan results.
    #
    # @note In both synchronous and asynchronous scans, each monitor
    #   thread maintains a minimum interval between scans, meaning
    #   calling this method may not initiate a scan on a particular server
    #   the very next instant.
    #
    # @example Force a full cluster scan.
    #   cluster.scan!
    #
    # @return [ true ] Always true.
    #
    # @since 2.0.0
    def scan!(sync=true)
      if sync
        servers_list.each do |server|
          server.scan!
        end
      else
        servers_list.each do |server|
          server.monitor.scan_semaphore.signal
        end
      end
      true
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
      addresses == other.addresses &&
        options.merge(server_selection_semaphore: nil) ==
          other.options.merge(server_selection_semaphore: nil)
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
    # @option options [ Boolean ] :monitor For internal driver use only:
    #   whether to monitor the newly added server.
    #
    # @return [ Server ] The newly added server, if not present already.
    #
    # @since 2.0.0
    def add(host, add_options=nil)
      address = Address.new(host, options)
      if !addresses.include?(address)
        server = Server.new(address, self, @monitoring, event_listeners, options.merge(
          monitor: false))
        @update_lock.synchronize { @servers.push(server) }
        if add_options.nil? || add_options[:monitor] != false
          server.start_monitoring
        end
        server
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
        if server.connected?
          server.disconnect!
          publish_sdam_event(
            Monitoring::SERVER_CLOSED,
            Monitoring::Event::ServerClosed.new(address, topology)
          )
        end
      end
      removed_servers.any?
    end

    # @api private
    def update_topology(new_topology)
      old_topology = topology
      @topology = new_topology
      publish_sdam_event(
        Monitoring::TOPOLOGY_CHANGED,
        Monitoring::Event::TopologyChanged.new(old_topology, topology)
      )
    end

    # @api private
    def servers_list
      @update_lock.synchronize { @servers.dup }
    end

    # @api private
    attr_reader :sdam_flow_lock

    private

    # If options[:session] is set, validates that session and returns it.
    # If deployment supports sessions, creates a new session and returns it.
    # The session is implicit unless options[:implicit] is given.
    # If deployment does not support session, returns nil.
    #
    # @note This method will return nil if deployment has no data-bearing
    #   servers at the time of the call.
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

    # Returns whether the deployment (as this term is defined in the sessions
    # spec) supports sessions.
    #
    # @note If the cluster has no data bearing servers, for example because
    #   the deployment is in the middle of a failover, this method returns
    #   false.
    def sessions_supported?
      if topology.data_bearing_servers?
        return !!topology.logical_session_timeout
      end

      begin
        ServerSelector.get(mode: :primary_preferred).select_server(self)
        !!topology.logical_session_timeout
      rescue Error::NoServerAvailable
        false
      end
    end

    def pools
      @pools ||= {}
    end
  end
end

require 'mongo/cluster/sdam_flow'

# Copyright (C) 2014-2019 MongoDB, Inc.
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
    include ClusterTime::Consumer

    # The default number of legacy read retries.
    #
    # @since 2.1.1
    MAX_READ_RETRIES = 1

    # The default number of legacy write retries.
    #
    # @since 2.4.2
    MAX_WRITE_RETRIES = 1

    # The default read retry interval, in seconds, when using legacy read
    # retries.
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
    # @deprecated
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
    # @option options [ true | false ] :scan Whether to scan all seeds
    #   in constructor. The default in driver version 2.x is to do so;
    #   driver version 3.x will not scan seeds in constructor. Opt in to the
    #   new behavior by setting this option to false. *Note:* setting
    #   this option to nil enables scanning seeds in constructor in driver
    #   version 2.x. Driver version 3.x will recognize this option but
    #   will ignore it and will never scan seeds in the constructor.
    # @option options [ true | false ] :monitoring_io For internal driver
    #   use only. Set to false to prevent SDAM-related I/O from being
    #   done by this cluster or servers under it. Note: setting this option
    #   to false will make the cluster non-functional. It is intended for
    #   use in tests which manually invoke SDAM state transitions.
    # @option options [ true | false ] :cleanup For internal driver use only.
    #   Set to false to prevent endSessions command being sent to the server
    #   to clean up server sessions when the cluster is disconnected, and to
    #   to not start the periodic executor. If :monitoring_io is false,
    #   :cleanup automatically defaults to false as well.
    # @option options [ Float ] :heartbeat_frequency The interval, in seconds,
    #   for the server monitor to refresh its description via ismaster.
    # @option options [ Hash ] :resolv_options For internal driver use only.
    #   Options to pass through to Resolv::DNS constructor for SRV lookups.
    #
    # @since 2.0.0
    def initialize(seeds, monitoring, options = Options::Redacted.new)
      if seeds.nil?
        raise ArgumentError, 'Seeds cannot be nil'
      end

      if options[:monitoring_io] == false && !options.key?(:cleanup)
        options = options.dup
        options[:cleanup] = false
      end

      @servers = []
      @monitoring = monitoring
      @event_listeners = Event::Listeners.new
      @options = options.freeze
      @app_metadata = Server::AppMetadata.new(@options)
      @update_lock = Mutex.new
      @sdam_flow_lock = Mutex.new
      @cluster_time = nil
      @cluster_time_lock = Mutex.new
      @srv_monitor_lock = Mutex.new
      @server_selection_semaphore = Semaphore.new
      @topology = Topology.initial(self, monitoring, options)
      Session::SessionPool.create(self)

      # The opening topology is always unknown with no servers.
      # https://github.com/mongodb/specifications/pull/388
      opening_topology = Topology::Unknown.new(options, monitoring, self)

      publish_sdam_event(
        Monitoring::TOPOLOGY_OPENING,
        Monitoring::Event::TopologyOpening.new(opening_topology)
      )

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

      if options[:monitoring_io] == false
        # Omit periodic executor construction, because without servers
        # no commands can be sent to the cluster and there shouldn't ever
        # be anything that needs to be cleaned up.
        #
        # Omit monitoring individual servers and the legacy single round of
        # of SDAM on the main thread, as it would race with tests that mock
        # SDAM responses.
        @connecting = @connected = false
        return
      end

      # Need to record start time prior to starting monitoring
      start_time = Time.now

      servers.each do |server|
        server.start_monitoring
      end

      if options[:cleanup] != false
        @cursor_reaper = CursorReaper.new
        @socket_reaper = SocketReaper.new(self)
        @periodic_executor = PeriodicExecutor.new([
          @cursor_reaper, @socket_reaper,
        ], options)

        ObjectSpace.define_finalizer(self, self.class.finalize(
          {}, @periodic_executor, @session_pool))

        @periodic_executor.run!
      end

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
        deadline = start_time + server_selection_timeout
        # Wait for the first scan of each server to complete, for
        # backwards compatibility.
        # If any servers are discovered during this SDAM round we are going to
        # wait for these servers to also be queried, and so on, up to the
        # server selection timeout or the 3 second minimum.
        loop do
          # Ensure we do not try to read the servers list while SDAM is running
          servers = @sdam_flow_lock.synchronize do
            servers_list.dup
          end
          if servers.all? { |server| server.last_scan && server.last_scan >= start_time }
            break
          end
          if (time_remaining = deadline - Time.now) <= 0
            break
          end
          log_debug("Waiting for up to #{'%.2f' % time_remaining} seconds for servers to be scanned: #{summary}")
          # Since the semaphore may have been signaled between us checking
          # the servers list above and the wait call below, we should not
          # wait for the full remaining time - wait for up to 1 second, then
          # recheck the state.
          server_selection_semaphore.wait([time_remaining, 1].min)
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

    [:register_cursor, :schedule_kill_cursor, :unregister_cursor].each do |m|
      define_method(m) do |*args|
        if options[:cleanup] != false
          @cursor_reaper.send(m, *args)
        end
      end
    end

    # @api private
    attr_reader :srv_monitor

    # Get the maximum number of times the client can retry a read operation
    # when using legacy read retries.
    #
    # @note max_read_retries should be retrieved from the Client instance,
    #   not from a Cluster instance, because clusters may be shared between
    #   clients with different values for max read retries.
    #
    # @example Get the max read retries.
    #   cluster.max_read_retries
    #
    # @return [ Integer ] The maximum number of retries.
    #
    # @since 2.1.1
    # @deprecated
    def max_read_retries
      options[:max_read_retries] || MAX_READ_RETRIES
    end

    # Get the interval, in seconds, in which read retries when using legacy
    # read retries.
    #
    # @note read_retry_interval should be retrieved from the Client instance,
    #   not from a Cluster instance, because clusters may be shared between
    #   clients with different values for the read retry interval.
    #
    # @example Get the read retry interval.
    #   cluster.read_retry_interval
    #
    # @return [ Float ] The interval.
    #
    # @since 2.1.1
    # @deprecated
    def read_retry_interval
      options[:read_retry_interval] || READ_RETRY_INTERVAL
    end

    # Get the refresh interval for the server. This will be defined via an
    # option or will default to 10.
    #
    # @return [ Float ] The heartbeat interval, in seconds.
    #
    # @since 2.10.0
    # @api private
    def heartbeat_interval
      options[:heartbeat_frequency] || Server::Monitor::HEARTBEAT_FREQUENCY
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
      "servers=[#{servers_list.map(&:summary).join(',')}]>"
    end

    # @api private
    attr_reader :server_selection_semaphore

    # Finalize the cluster for garbage collection.
    #
    # @example Finalize the cluster.
    #   Cluster.finalize(pools)
    #
    # @param [ Hash<Address, Server::ConnectionPool> ] pools Ignored.
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
      end
    end

    # Close the cluster and disconnect all servers.
    #
    # @note Applications should call Client#close to disconnect from
    # the cluster rather than calling this method. This method is for
    # internal driver use only.
    #
    # @example Disconnect the cluster's servers.
    #   cluster.disconnect!
    #
    # @param [ Boolean ] wait Whether to wait for background threads to
    #   finish running.
    #
    # @return [ true ] Always true.
    #
    # @since 2.1.0
    def disconnect!(wait=false)
      unless @connecting || @connected
        return true
      end
      if options[:cleanup] != false
        session_pool.end_sessions
        @periodic_executor.stop!
      end
      @srv_monitor_lock.synchronize do
        if @srv_monitor
          @srv_monitor.stop!
        end
      end
      @servers.each do |server|
        if server.connected?
          server.disconnect!(wait)
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
          if server.monitor
            server.monitor.scan!
          else
            log_warn("Synchronous scan requested on cluster #{summary} but server #{server} has no monitor")
          end
        end
      else
        servers_list.each do |server|
          server.scan_semaphore.signal
        end
      end
      true
    end

    # Runs SDAM flow on the cluster.
    #
    # This method can be invoked to process a new server description returned
    # by the server on a monitoring or non-monitoring connection, and also
    # by the driver when it marks a server unknown as a result of a (network)
    # error.
    #
    # @param [ Server::Description ] previous_desc Previous server description.
    # @param [ Server::Description ] updated_desc The changed description.
    # @param [ Hash ] options Options.
    #
    # @option options [ true | false ] :keep_connection_pool Usually when the
    #   new server description is unknown, the connection pool on the
    #   respective server is cleared. Set this option to true to keep the
    #   existing connection pool (required when handling not master errors
    #   on 4.2+ servers).
    #
    # @api private
    def run_sdam_flow(previous_desc, updated_desc, options = {})
      @sdam_flow_lock.synchronize do
        flow = SdamFlow.new(self, previous_desc, updated_desc)
        flow.server_description_changed

        # SDAM flow may alter the updated description - grab the final
        # version for the purposes of broadcasting if a server is available
        updated_desc = flow.updated_desc

        unless options[:keep_connection_pool]
          if flow.became_unknown?
            servers_list.each do |server|
              if server.address == updated_desc.address
                server.clear_connection_pool
              end
            end
          end
        end
      end

      # Some updated descriptions, e.g. a mismatched me one, result in the
      # server whose description we are processing being removed from
      # the topology. When this happens, the server's monitoring thread gets
      # killed. As a result, any code after the flow invocation may not run
      # a particular monitor instance, hence there should generally not be
      # any code in this method past the flow invocation.
      #
      # However, this broadcast call can be here because if the monitoring
      # thread got killed the server should have been closed and no client
      # should be currently waiting for it, thus not signaling the semaphore
      # shouldn't cause any problems.
      unless updated_desc.unknown?
        server_selection_semaphore.broadcast
      end

      check_and_start_srv_monitor
    end

    # Sets the list of servers to the addresses in the provided list of address
    # strings.
    #
    # This method is called by the SRV monitor after receiving new DNS records
    # for the monitored hostname.
    #
    # Removes servers in the cluster whose addresses are not in the passed
    # list of server addresses, and adds servers for any addresses in the
    # argument which are not already in the cluster.
    #
    # @param [ Array<String> ] server_address_strs List of server addresses
    #    to sync the cluster servers to.
    #
    # @api private
    def set_server_list(server_address_strs)
      @sdam_flow_lock.synchronize do
        server_address_strs.each do |address_str|
          unless servers_list.any? { |server| server.address.seed == address_str }
            add(address_str)
          end
        end

        servers_list.each do |server|
          unless server_address_strs.any? { |address_str| server.address.seed == address_str }
            remove(server.address.seed)
          end
        end
      end
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
    # @param [ true, false ] ping Whether to ping the server before selection.
    #   Deprecated and ignored.
    # @param [ Session | nil ] session Optional session to take into account
    #   for mongos pinning.
    #
    # @return [ Mongo::Server ] A primary server.
    #
    # @since 2.0.0
    def next_primary(ping = nil, session = nil)
      ServerSelector.primary.select_server(self, nil, session)
    end

    # Get the connection pool for the server.
    #
    # @example Get the connection pool.
    #   cluster.pool(server)
    #
    # @param [ Server ] server The server.
    #
    # @return [ Server::ConnectionPool ] The connection pool.
    #
    # @since 2.2.0
    # @deprecated
    def pool(server)
      server.pool
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
          advance_cluster_time(cluster_time_doc)
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
    #
    # @note This method returns as soon as the driver connects to any single
    #   server in the deployment. Whether deployment overall supports sessions
    #   can change depending on how many servers have been contacted, if
    #   the servers are configured differently.
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

    # @api private
    def check_and_start_srv_monitor
      return unless topology.is_a?(Topology::Sharded) && options[:srv_uri]
      @srv_monitor_lock.synchronize do
        unless @srv_monitor
          monitor_options = options.merge(
            timeout: options[:connect_timeout] || Server::CONNECT_TIMEOUT)
          @srv_monitor = _srv_monitor = SrvMonitor.new(self, monitor_options)
          finalizer = lambda do
            _srv_monitor.stop!
          end
          ObjectSpace.define_finalizer(self, finalizer)
        end
        @srv_monitor.run!
      end
    end
  end
end

require 'mongo/cluster/sdam_flow'
require 'mongo/cluster/srv_monitor'

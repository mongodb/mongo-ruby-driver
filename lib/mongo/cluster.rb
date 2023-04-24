# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2014-2020 MongoDB Inc.
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
    #
    # @option options [ true | false ] :direct_connection Whether to connect
    #   directly to the specified seed, bypassing topology discovery. Exactly
    #   one seed must be provided.
    # @option options [ Symbol ] :connect Deprecated - use :direct_connection
    #   option instead of this option. The connection method to use. This
    #   forces the cluster to behave in the specified way instead of
    #   auto-discovering. One of :direct, :replica_set, :sharded
    # @option options [ Symbol ] :replica_set The name of the replica set to
    #   connect to. Servers not in this replica set will be ignored.
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
    #   for the server monitor to refresh its description via hello.
    # @option options [ Hash ] :resolv_options For internal driver use only.
    #   Options to pass through to Resolv::DNS constructor for SRV lookups.
    # @option options [ Hash ] :server_api The requested server API version.
    #   This hash can have the following items:
    #   - *:version* -- string
    #   - *:strict* -- boolean
    #   - *:deprecation_errors* -- boolean
    #
    # @since 2.0.0
    def initialize(seeds, monitoring, options = Options::Redacted.new)
      if seeds.nil?
        raise ArgumentError, 'Seeds cannot be nil'
      end

      options = options.dup
      if options[:monitoring_io] == false && !options.key?(:cleanup)
        options[:cleanup] = false
      end
      @options = options.freeze

      # @update_lock covers @servers, @connecting, @connected, @topology and
      # @sessions_supported. Generally instance variables that do not have a
      # designated for them lock should only be modified under the update lock.
      # Note that topology change is locked by @update_lock and not by
      # @sdam_flow_lock.
      @update_lock = Mutex.new
      @servers = []
      @monitoring = monitoring
      @event_listeners = Event::Listeners.new
      @app_metadata = Server::AppMetadata.new(@options.merge(purpose: :application))
      @monitor_app_metadata = Server::Monitor::AppMetadata.new(@options.merge(purpose: :monitor))
      @push_monitor_app_metadata = Server::Monitor::AppMetadata.new(@options.merge(purpose: :push_monitor))
      @cluster_time_lock = Mutex.new
      @cluster_time = nil
      @srv_monitor_lock = Mutex.new
      @srv_monitor = nil
      @server_selection_semaphore = Semaphore.new
      @topology = Topology.initial(self, monitoring, options)
      # State change lock is similar to the sdam flow lock, but is designed
      # to serialize state changes initated by consumers of Cluster
      # (e.g. application connecting or disconnecting the cluster), so that
      # e.g. an application calling disconnect-connect-disconnect rapidly
      # does not put the cluster into an inconsistent state.
      # Monitoring updates performed internally by the driver do not take
      # the state change lock.
      @state_change_lock = Mutex.new
      # @sdam_flow_lock covers just the sdam flow. Note it does not apply
      # to @topology replacements which are done under @update_lock.
      @sdam_flow_lock = Mutex.new
      Session::SessionPool.create(self)

      if seeds.empty? && load_balanced?
        raise ArgumentError, 'Load-balanced clusters with no seeds are prohibited'
      end

      # The opening topology is always unknown with no servers.
      # https://github.com/mongodb/specifications/pull/388
      opening_topology = Topology::Unknown.new(options, monitoring, self)

      publish_sdam_event(
        Monitoring::TOPOLOGY_OPENING,
        Monitoring::Event::TopologyOpening.new(opening_topology)
      )

      @seeds = seeds = seeds.uniq
      servers = seeds.map do |seed|
        # Server opening events must be sent after topology change events.
        # Therefore separate server addition, done here before topology change
        # event is published, from starting to monitor the server which is
        # done later.
        add(seed, monitor: false)
      end

      if seeds.size >= 1
        # Recreate the topology to get the current server list into it
        recreate_topology(topology, opening_topology)
      end

      if load_balanced?
        # We are required by the specifications to produce certain SDAM events
        # when in load-balanced topology.
        # These events don't make a lot of sense from the standpoint of the
        # driver's SDAM implementation, nor from the standpoint of the
        # driver's load balancer implementation.
        # They are just required boilerplate.
        #
        # Note that this call must be done above the monitoring_io check
        # because that short-circuits the rest of the constructor.
        fabricate_lb_sdam_events_and_set_server_type
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

      # Update instance variables prior to starting monitoring threads.
      @connecting = false
      @connected = true

      if options[:cleanup] != false
        @cursor_reaper = CursorReaper.new(self)
        @socket_reaper = SocketReaper.new(self)
        @periodic_executor = PeriodicExecutor.new([
          @cursor_reaper, @socket_reaper,
        ], options)

        @periodic_executor.run!
      end

      unless load_balanced?
        # Need to record start time prior to starting monitoring
        start_monotime = Utils.monotonic_time

        servers.each do |server|
          server.start_monitoring
        end

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
          deadline = start_monotime + server_selection_timeout
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
            if servers.all? { |server| server.last_scan_monotime && server.last_scan_monotime >= start_monotime }
              break
            end
            if (time_remaining = deadline - Utils.monotonic_time) <= 0
              break
            end
            log_debug("Waiting for up to #{'%.2f' % time_remaining} seconds for servers to be scanned: #{summary}")
            # Since the semaphore may have been signaled between us checking
            # the servers list above and the wait call below, we should not
            # wait for the full remaining time - wait for up to 0.5 second, then
            # recheck the state.
            begin
              server_selection_semaphore.wait([time_remaining, 0.5].min)
            rescue ::Timeout::Error
              # nothing
            end
          end
        end

        start_stop_srv_monitor
      end
    end

    # Create a cluster for the provided client, for use when we don't want the
    # client's original cluster instance to be the same.
    #
    # @example Create a cluster for the client.
    #   Cluster.create(client)
    #
    # @param [ Client ] client The client to create on.
    # @param [ Monitoring | nil ] monitoring. The monitoring instance to use
    #   with the new cluster. If nil, a new instance of Monitoring will be
    #   created.
    #
    # @return [ Cluster ] The cluster.
    #
    # @since 2.0.0
    # @api private
    def self.create(client, monitoring: nil)
      cluster = Cluster.new(
        client.cluster.addresses.map(&:to_s),
        monitoring || Monitoring.new,
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

    # @return [ Mongo::Server::AppMetadata ] The application metadata, used for
    #   connection handshakes.
    #
    # @since 2.4.0
    attr_reader :app_metadata

    # @api private
    attr_reader :monitor_app_metadata

    # @api private
    attr_reader :push_monitor_app_metadata

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

    # Returns whether the cluster is configured to be in the load-balanced
    # topology.
    #
    # @return [ true | false ] Whether the topology is load-balanced.
    def load_balanced?
      topology.is_a?(Topology::LoadBalanced)
    end

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
      options[:heartbeat_frequency] || Server::Monitor::DEFAULT_HEARTBEAT_INTERVAL
    end

    # Whether the cluster object is in the process of connecting to its cluster.
    #
    # @return [ true|false ] Whether the cluster is connecting.
    #
    # @api private
    def connecting?
      @update_lock.synchronize do
        !!@connecting
      end
    end

    # Whether the cluster object is connected to its cluster.
    #
    # @return [ true|false ] Whether the cluster is connected.
    #
    # @api private
    # @since 2.7.0
    def connected?
      @update_lock.synchronize do
        !!@connected
      end
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
      topology.servers(servers_list)
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
      servers_list.map(&:address)
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

    # Closes the cluster.
    #
    # @note Applications should call Client#close to disconnect from
    # the cluster rather than calling this method. This method is for
    # internal driver use only.
    #
    # Disconnects all servers in the cluster, publishing appropriate SDAM
    # events in the process. Stops SRV monitoring if it is active.
    # Marks the cluster disconnected.
    #
    # A closed cluster is no longer usable. If the client is reconnected,
    # it will create a new cluster instance.
    #
    # @return [ nil ] Always nil.
    #
    # @api private
    def close
      @state_change_lock.synchronize do
        unless connecting? || connected?
          return nil
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
            server.close
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
        @update_lock.synchronize do
          @connecting = @connected = false
        end
      end
      nil
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
      @state_change_lock.synchronize do
        @update_lock.synchronize do
          @connecting = true
        end
        scan!
        servers.each do |server|
          server.reconnect!
        end
        @periodic_executor.restart!
        @srv_monitor_lock.synchronize do
          if @srv_monitor
            @srv_monitor.run!
          end
        end
        @update_lock.synchronize do
          @connecting = false
          @connected = true
        end
      end
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
    # @option options [ true | false ] :awaited Whether the updated description
    #   was a result of processing an awaited hello.
    # @option options [ Object ] :service_id Change state for the specified
    #   service id only.
    # @option options [ Mongo::Error | nil ] :scan_error The error encountered
    #   while scanning, or nil if no error was raised.
    #
    # @api private
    def run_sdam_flow(previous_desc, updated_desc, options = {})
      if load_balanced?
        if updated_desc.config.empty?
          unless options[:keep_connection_pool]
            servers_list.each do |server|
              # TODO should service id be taken out of updated_desc?
              # We could also assert that
              # options[:service_id] == updated_desc.service_id
              err = options[:scan_error]
              interrupt = err && (err.is_a?(Error::SocketError) || err.is_a?(Error::SocketTimeoutError))
              server.clear_connection_pool(service_id: options[:service_id], interrupt_in_use_connections: interrupt)
            end
          end
        end
        return
      end

      @sdam_flow_lock.synchronize do
        flow = SdamFlow.new(self, previous_desc, updated_desc,
          awaited: options[:awaited])
        flow.server_description_changed

        # SDAM flow may alter the updated description - grab the final
        # version for the purposes of broadcasting if a server is available
        updated_desc = flow.updated_desc

        unless options[:keep_connection_pool]
          if flow.became_unknown?
            servers_list.each do |server|
              if server.address == updated_desc.address
                err = options[:scan_error]
                interrupt = err && (err.is_a?(Error::SocketError) || err.is_a?(Error::SocketTimeoutError))
                server.clear_connection_pool(interrupt_in_use_connections: interrupt)
              end
            end
          end
        end

        start_stop_srv_monitor
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
        # If one of the new addresses is not in the current servers list,
        # add it to the servers list.
        server_address_strs.each do |address_str|
          unless servers_list.any? { |server| server.address.seed == address_str }
            add(address_str)
          end
        end

        # If one of the servers' addresses are not in the new address list,
        # remove that server from the servers list.
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
    # auto-discovery of new servers when an existing server executes a hello
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
        opts = options.merge(monitor: false)
        # If we aren't starting the montoring threads, we also don't want to
        # start the pool's populator thread.
        opts.merge!(populator_io: false) unless options.fetch(:monitoring_io, true)
        # Note that in a load-balanced topology, every server must be a
        # load balancer (load_balancer: true is specified in the options)
        # but this option isn't set here because we are required by the
        # specifications to pretent the server started out as an unknown one
        # and publish server description change event into the load balancer
        # one. The actual correct description for this server will be set
        # by the fabricate_lb_sdam_events_and_set_server_type method.
        server = Server.new(address, self, @monitoring, event_listeners, opts)
        @update_lock.synchronize do
          # Need to recheck whether server is present in @servers, because
          # the previous check was not under a lock.
          # Since we are under the update lock here, we cannot call servers_list.
          return if @servers.map(&:address).include?(address)

          @servers.push(server)
        end
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
    # @param [ true | false ] disconnect Whether to disconnect the servers
    #   being removed. For internal driver use only.
    #
    # @return [ Array<Server> | true | false ] If disconnect is any value other
    #   than false, including nil, returns whether any servers were removed.
    #   If disconnect is false, returns an array of servers that were removed
    #   (and should be disconnected by the caller).
    #
    # @note The return value of this method is not part of the driver's
    #   public API.
    #
    # @since 2.0.0
    def remove(host, disconnect: true)
      address = Address.new(host)
      removed_servers = []
      @update_lock.synchronize do
        @servers.delete_if do |server|
          (server.address == address).tap do |delete|
            if delete
              removed_servers << server
            end
          end
        end
      end
      if disconnect != false
        removed_servers.each do |server|
          disconnect_server_if_connected(server)
        end
      end
      if disconnect != false
        removed_servers.any?
      else
        removed_servers
      end
    end

    # @api private
    def update_topology(new_topology)
      old_topology = nil
      @update_lock.synchronize do
        old_topology = topology
        @topology = new_topology
      end

      # If new topology has data bearing servers, we know for sure whether
      # sessions are supported - update our cached value.
      # If new topology has no data bearing servers, leave the old value
      # as it is and sessions_supported? method will perform server selection
      # to try to determine session support accurately, falling back to the
      # last known value.
      if topology.data_bearing_servers?
        sessions_supported = !!topology.logical_session_timeout
        @update_lock.synchronize do
          @sessions_supported = sessions_supported
        end
      end

      publish_sdam_event(
        Monitoring::TOPOLOGY_CHANGED,
        Monitoring::Event::TopologyChanged.new(old_topology, topology)
      )
    end

    # @api private
    def servers_list
      @update_lock.synchronize do
        @servers.dup
      end
    end

    # @api private
    def disconnect_server_if_connected(server)
      if server.connected?
        server.clear_description
        server.disconnect!
        publish_sdam_event(
          Monitoring::SERVER_CLOSED,
          Monitoring::Event::ServerClosed.new(server.address, topology)
        )
      end
    end

    # Raises Error::SessionsNotAvailable if the deployment that the driver
    # is connected to does not support sessions.
    #
    # Session support may change over time, for example due to servers in the
    # deployment being upgraded or downgraded.  If the client isn't connected to
    # any servers and doesn't find any servers
    # for the duration of server selection timeout, this method will raise
    # NoServerAvailable. This method is called from the operation execution flow,
    # and if it raises NoServerAvailable the entire operation will fail
    # with that exception, since the operation execution has waited for
    # the server selection timeout for any server to become available
    # (which would be a superset of the servers suitable for the operation being
    # attempted) and none materialized.
    #
    # @raise [ Error::SessionsNotAvailable ] If the deployment that the driver
    #   is connected to does not support sessions.
    # @raise [ Error::NoServerAvailable ] If the client isn't connected to
    #   any servers and doesn't find any servers for the duration of
    #   server selection timeout.
    #
    # @api private
    def validate_session_support!
      if topology.is_a?(Topology::LoadBalanced)
        return
      end

      @state_change_lock.synchronize do
        @sdam_flow_lock.synchronize do
          if topology.data_bearing_servers?
            unless topology.logical_session_timeout
              raise_sessions_not_supported
            end
          end
        end
      end

      # No data bearing servers known - perform server selection to try to
      # get a response from at least one of them, to return an accurate
      # assessment of whether sessions are currently supported.
      ServerSelector.get(mode: :primary_preferred).select_server(self)
      @state_change_lock.synchronize do
        @sdam_flow_lock.synchronize do
          unless topology.logical_session_timeout
            raise_sessions_not_supported
          end
        end
      end
    end

    private

    # @api private
    def start_stop_srv_monitor
      # SRV URI is either always given or not for a given cluster, if one
      # wasn't given we shouldn't ever have an SRV monitor to manage.
      return unless options[:srv_uri]

      if topology.is_a?(Topology::Sharded) || topology.is_a?(Topology::Unknown)
        # Start SRV monitor
        @srv_monitor_lock.synchronize do
          unless @srv_monitor
            monitor_options = Utils.shallow_symbolize_keys(options.merge(
              timeout: options[:connect_timeout] || Server::CONNECT_TIMEOUT))
            @srv_monitor = _srv_monitor = Srv::Monitor.new(self, **monitor_options)
          end
          @srv_monitor.run!
        end
      else
        # Stop SRV monitor if running. This path is taken when the client
        # is given an SRV URI to a standalone/replica set; when the topology
        # is discovered, since it's not a sharded cluster, the SRV monitor
        # needs to be stopped.
        @srv_monitor_lock.synchronize do
          if @srv_monitor
            @srv_monitor.stop!
          end
        end
      end
    end

    def raise_sessions_not_supported
      # Intentionally using @servers instead of +servers+ here because we
      # are supposed to be already holding the @update_lock and we cannot
      # recursively acquire it again.
      offending_servers = @servers.select do |server|
        server.description.data_bearing? && server.logical_session_timeout.nil?
      end
      reason = if offending_servers.empty?
        "There are no known data bearing servers (current seeds: #{@servers.map(&:address).map(&:seed).join(', ')})"
      else
        "The following servers have null logical session timeout: #{offending_servers.map(&:address).map(&:seed).join(', ')}"
      end
      msg = "The deployment that the driver is connected to does not support sessions: #{reason}"
      raise Error::SessionsNotSupported, msg
    end

    def fabricate_lb_sdam_events_and_set_server_type
      # Although there is no monitoring connection in load balanced mode,
      # we must emit the following series of SDAM events.
      server = @servers.first
      # We are guaranteed to have the server here.
      server.publish_opening_event
      server_desc = server.description
      # This is where a load balancer actually gets its correct server
      # description.
      server.update_description(
        Server::Description.new(server.address, {},
          load_balancer: true,
          force_load_balancer: options[:connect] == :load_balanced,
        )
      )
      publish_sdam_event(
        Monitoring::SERVER_DESCRIPTION_CHANGED,
        Monitoring::Event::ServerDescriptionChanged.new(
          server.address,
          topology,
          server_desc,
          server.description
        )
      )
      recreate_topology(topology, topology)
    end

    def recreate_topology(new_topology_template, previous_topology)
      @topology = topology.class.new(new_topology_template.options, new_topology_template.monitoring, self)
      publish_sdam_event(
        Monitoring::TOPOLOGY_CHANGED,
        Monitoring::Event::TopologyChanged.new(previous_topology, @topology)
      )
    end
  end
end

require 'mongo/cluster/sdam_flow'

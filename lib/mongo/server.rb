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

module Mongo

  # Represents a single server on the server side that can be standalone, part of
  # a replica set, or a mongos.
  #
  # @since 2.0.0
  class Server
    extend Forwardable
    include Monitoring::Publishable
    include Event::Publisher

    # The default time in seconds to timeout a connection attempt.
    #
    # @since 2.4.3
    CONNECT_TIMEOUT = 10.freeze

    # Instantiate a new server object. Will start the background refresh and
    # subscribe to the appropriate events.
    #
    # @api private
    #
    # @example Initialize the server.
    #   Mongo::Server.new('127.0.0.1:27017', cluster, monitoring, listeners)
    #
    # @note Server must never be directly instantiated outside of a Cluster.
    #
    # @param [ Address ] address The host:port address to connect to.
    # @param [ Cluster ] cluster  The cluster the server belongs to.
    # @param [ Monitoring ] monitoring The monitoring.
    # @param [ Event::Listeners ] event_listeners The event listeners.
    # @param [ Hash ] options The server options.
    #
    # @option options [ Boolean ] :monitor For internal driver use only:
    #   whether to monitor the server after instantiating it.
    # @option options [ true, false ] :monitoring_io For internal driver
    #   use only. Set to false to prevent SDAM-related I/O from being
    #   done by this server. Note: setting this option to false will make
    #   the server non-functional. It is intended for use in tests which
    #   manually invoke SDAM state transitions.
    # @option options [ true, false ] :populator_io For internal driver
    #   use only. Set to false to prevent the populator threads from being
    #   created and started in the server's connection pool. It is intended
    #   for use in tests that also turn off monitoring_io, unless the populator
    #   is explicitly needed. If monitoring_io is off, but the populator_io
    #   is on, the populator needs to be manually closed at the end of the
    #   test, since a cluster without monitoring is considered not connected,
    #   and thus will not clean up the connection pool populator threads on
    #   close.
    # @option options [ true | false ] :load_balancer Whether this server
    #   is a load balancer.
    # @option options [ String ] :connect The client connection mode.
    #
    # @since 2.0.0
    def initialize(address, cluster, monitoring, event_listeners, options = {})
      @address = address
      @cluster = cluster
      @monitoring = monitoring
      options = options.dup
      _monitor = options.delete(:monitor)
      @options = options.freeze
      @event_listeners = event_listeners
      @connection_id_gen = Class.new do
        include Id
      end
      @scan_semaphore = DistinguishingSemaphore.new
      @round_trip_time_averager = RoundTripTimeAverager.new
      @description = Description.new(address, {},
        load_balancer: !!@options[:load_balancer],
        force_load_balancer: force_load_balancer?,
      )
      @last_scan = nil
      @last_scan_monotime = nil
      unless options[:monitoring_io] == false
        @monitor = Monitor.new(self, event_listeners, monitoring,
          options.merge(
            app_metadata: cluster.monitor_app_metadata,
            push_monitor_app_metadata: cluster.push_monitor_app_metadata,
            heartbeat_interval: cluster.heartbeat_interval,
        ))
        unless _monitor == false
          start_monitoring
        end
      end
      @connected = true
      @pool_lock = Mutex.new
    end

    # @return [ String ] The configured address for the server.
    attr_reader :address

    # @return [ Cluster ] cluster The server cluster.
    attr_reader :cluster

    # @return [ nil | Monitor ] monitor The server monitor. nil if the servenr
    #   was created with monitoring_io: false option.
    attr_reader :monitor

    # @return [ Hash ] The options hash.
    attr_reader :options

    # @return [ Monitoring ] monitoring The monitoring.
    attr_reader :monitoring

    # @return [ Server::Description ] description The server
    #   description the monitor refreshes.
    attr_reader :description

    # Returns whether this server is forced to be a load balancer.
    #
    # @return [ true | false ] Whether this server is forced to be a load balancer.
    #
    # @api private
    def force_load_balancer?
      options[:connect] == :load_balanced
    end

    # @return [ Time | nil ] last_scan The time when the last server scan
    #   completed, or nil if the server has not been scanned yet.
    #
    # @since 2.4.0
    def last_scan
      if description && !description.config.empty?
        description.last_update_time
      else
        @last_scan
      end
    end

    # @return [ Float | nil ] last_scan_monotime The monotonic time when the last server scan
    #   completed, or nil if the server has not been scanned yet.
    # @api private
    def last_scan_monotime
      if description && !description.config.empty?
        description.last_update_monotime
      else
        @last_scan_monotime
      end
    end


    # @deprecated
    def heartbeat_frequency
      cluster.heartbeat_interval
    end

    # @deprecated
    alias :heartbeat_frequency_seconds :heartbeat_frequency

    # Performs an immediate, synchronous check of the server.
    #
    # @deprecated
    def_delegators :monitor, :scan!

    # The compressor negotiated by the server monitor, if any.
    #
    # This attribute is nil if no server check has not yet completed, and if
    # no compression was negatiated.
    #
    # @note Compression is negotiated for each connection separately.
    #
    # @return [ String | nil ] The negotiated compressor.
    #
    # @deprecated
    def compressor
      if monitor
        monitor.compressor
      else
        nil
      end
    end

    # Delegate convenience methods to the monitor description.
    def_delegators :description,
                   :arbiter?,
                   :features,
                   :ghost?,
                   :max_wire_version,
                   :max_write_batch_size,
                   :max_bson_object_size,
                   :max_message_size,
                   :tags,
                   :average_round_trip_time,
                   :mongos?,
                   :other?,
                   :primary?,
                   :replica_set_name,
                   :secondary?,
                   :standalone?,
                   :unknown?,
                   :load_balancer?,
                   :last_write_date,
                   :logical_session_timeout

    # Get the app metadata from the cluster.
    def_delegators :cluster,
                   :app_metadata,
                   :cluster_time,
                   :update_cluster_time

    # @api private
    def_delegators :cluster,
                   :monitor_app_metadata,
                   :push_monitor_app_metadata

    def_delegators :features,
                   :check_driver_support!

    # @return [ Semaphore ] Semaphore to signal to request an immediate scan
    #   of this server by its monitor, if one is running.
    #
    # @api private
    attr_reader :scan_semaphore

    # @return [ RoundTripTimeAverager ] Round trip time averager object.
    # @api private
    attr_reader :round_trip_time_averager

    # Is this server equal to another?
    #
    # @example Is the server equal to the other?
    #   server == other
    #
    # @param [ Object ] other The object to compare to.
    #
    # @return [ true, false ] If the servers are equal.
    #
    # @since 2.0.0
    def ==(other)
      return false unless other.is_a?(Server)
      address == other.address
    end

    # Determine if a connection to the server is able to be established and
    # messages can be sent to it.
    #
    # @example Is the server connectable?
    #   server.connectable?
    #
    # @return [ true, false ] If the server is connectable.
    #
    # @since 2.1.0
    #
    # @deprecated No longer necessary with Server Selection specification.
    def connectable?; end

    # Disconnect the driver from this server.
    #
    # Disconnects all idle connections to this server in its connection pool,
    # if any exist. Stops the populator of the connection pool, if it is
    # running. Does not immediately close connections which are presently
    # checked out (i.e. in use) - such connections will be closed when they
    # are returned to their respective connection pools. Stop the server's
    # background monitor.
    #
    # @return [ true ] Always true.
    #
    # @since 2.0.0
    def disconnect!
      if monitor
        monitor.stop!
      end

      @connected = false

      # The current CMAP spec requires a pool to be mostly unusable
      # if its server is unknown (or, therefore, disconnected).
      # However any outstanding operations should continue to completion,
      # and their connections need to be checked into the pool to be
      # torn down. Because of this cleanup requirement we cannot just
      # close the pool and set it to nil here, to be recreated the next
      # time the server is discovered.
      pool_internal&.clear

      true
    end

    def close
      if monitor
        monitor.stop!
      end

      @connected = false

      _pool = nil
      @pool_lock.synchronize do
        _pool, @pool = @pool, nil
      end

      # TODO: change this to _pool.close in RUBY-3174.
      # Clear the pool. If the server is not unknown then the
      # pool will stay ready. Stop the background populator thread.
      _pool&.close(stay_ready: true)

      nil
    end

    # Whether the server is connected.
    #
    # @return [ true|false ] Whether the server is connected.
    #
    # @api private
    # @since 2.7.0
    def connected?
      @connected
    end

    # Start monitoring the server.
    #
    # Used internally by the driver to add a server to a cluster
    # while delaying monitoring until the server is in the cluster.
    #
    # @api private
    def start_monitoring
      publish_opening_event
      if options[:monitoring_io] != false
        monitor.run!
      end
    end

    # Publishes the server opening event.
    #
    # @api private
    def publish_opening_event
      publish_sdam_event(
        Monitoring::SERVER_OPENING,
        Monitoring::Event::ServerOpening.new(address, cluster.topology)
      )
    end

    # Get a pretty printed server inspection.
    #
    # @example Get the server inspection.
    #   server.inspect
    #
    # @return [ String ] The nice inspection string.
    #
    # @since 2.0.0
    def inspect
      "#<Mongo::Server:0x#{object_id} address=#{address.host}:#{address.port} #{status}>"
    end

    # @return [ String ] String representing server status (e.g. PRIMARY).
    #
    # @api private
    def status
      case
      when load_balancer?
        'LB'
      when primary?
        'PRIMARY'
      when secondary?
        'SECONDARY'
      when standalone?
        'STANDALONE'
      when arbiter?
        'ARBITER'
      when ghost?
        'GHOST'
      when other?
        'OTHER'
      when mongos?
        'MONGOS'
      when unknown?
        'UNKNOWN'
      else
        # Since the summary method is often used for debugging, do not raise
        # an exception in case none of the expected types matched
        nil
      end
    end

    # @note This method is experimental and subject to change.
    #
    # @api experimental
    # @since 2.7.0
    def summary
      status = self.status || ''
      if replica_set_name
        status += " replica_set=#{replica_set_name}"
      end

      unless monitor&.running?
        status += " NO-MONITORING"
      end

      if @pool
        status += " pool=#{@pool.summary}"
      end

      address_bit = if address
        "#{address.host}:#{address.port}"
      else
        'nil'
      end

      "#<Server address=#{address_bit} #{status}>"
    end

    # Get the connection pool for this server.
    #
    # @example Get the connection pool for the server.
    #   server.pool
    #
    # @return [ Mongo::Server::ConnectionPool ] The connection pool.
    #
    # @since 2.0.0
    def pool
      if unknown?
        raise Error::ServerNotUsable, address
      end

      @pool_lock.synchronize do
        opts = connected? ? options : options.merge(populator_io: false)
        @pool ||= ConnectionPool.new(self, opts).tap do |pool|
          pool.ready
        end
      end
    end

    # Internal driver method to retrieve the connection pool for this server.
    #
    # Unlike +pool+, +pool_internal+ will not create a pool if one does not
    # already exist.
    #
    # @return [ Server::ConnectionPool | nil ] The connection pool, if one exists.
    #
    # @api private
    def pool_internal
      @pool_lock.synchronize do
        @pool
      end
    end

    # Determine if the provided tags are a subset of the server's tags.
    #
    # @example Are the provided tags a subset of the server's tags.
    #   server.matches_tag_set?({ 'rack' => 'a', 'dc' => 'nyc' })
    #
    # @param [ Hash ] tag_set The tag set to compare to the server's tags.
    #
    # @return [ true, false ] If the provided tags are a subset of the server's tags.
    #
    # @since 2.0.0
    def matches_tag_set?(tag_set)
      tag_set.keys.all? do |k|
        tags[k] && tags[k] == tag_set[k]
      end
    end

    # Restart the server monitor.
    #
    # @example Restart the server monitor.
    #   server.reconnect!
    #
    # @return [ true ] Always true.
    #
    # @since 2.1.0
    def reconnect!
      if options[:monitoring_io] != false
        monitor.restart!
      end
      @connected = true
    end

    # Execute a block of code with a connection, that is checked out of the
    # server's pool and then checked back in.
    #
    # @example Send a message with the connection.
    #   server.with_connection do |connection|
    #     connection.dispatch([ command ])
    #   end
    #
    # @return [ Object ] The result of the block execution.
    #
    # @since 2.3.0
    def with_connection(connection_global_id: nil, &block)
      pool.with_connection(connection_global_id: connection_global_id, &block)
    end

    # Handle handshake failure.
    #
    # @since 2.7.0
    # @api private
    def handle_handshake_failure!
      yield
    rescue Mongo::Error::SocketError, Mongo::Error::SocketTimeoutError => e
      unknown!(
        generation: e.generation,
        service_id: e.service_id,
        stop_push_monitor: true,
      )
      raise
    end

    # Handle authentication failure.
    #
    # @example Handle possible authentication failure.
    #   server.handle_auth_failure! do
    #     Auth.get(user).login(self)
    #   end
    #
    # @raise [ Auth::Unauthorized ] If the authentication failed.
    #
    # @return [ Object ] The result of the block execution.
    #
    # @since 2.3.0
    def handle_auth_failure!
      yield
    rescue Mongo::Error::SocketTimeoutError
      # possibly cluster is slow, do not give up on it
      raise
    rescue Mongo::Error::SocketError, Auth::Unauthorized => e
      # non-timeout network error or auth error, clear the pool and mark the
      # topology as unknown
      unknown!(
        generation: e.generation,
        service_id: e.service_id,
        stop_push_monitor: true,
      )
      raise
    end

    # Whether the server supports modern read retries.
    #
    # @api private
    def retry_reads?
      !!(features.sessions_enabled? && logical_session_timeout)
    end

    # Will writes sent to this server be retried.
    #
    # @example Will writes be retried.
    #   server.retry_writes?
    #
    # @return [ true, false ] If writes will be retried.
    #
    # @note Retryable writes are only available on server versions 3.6+ and with
    #   sharded clusters or replica sets.
    #
    # @note Some of the conditions in this method automatically return false for
    #       for load balanced topologies. The conditions in this method should
    #       always be true, since load-balanced topologies are only available on
    #       MongoDB 5.0+, and not for standalone topologies. Therefore, we can
    #       assume that retry writes are enabled.
    #
    # @since 2.5.0
    def retry_writes?
      !!(features.sessions_enabled? && logical_session_timeout && !standalone?) || load_balancer?
    end

    # Marks server unknown and publishes the associated SDAM event
    # (server description changed).
    #
    # If the generation is passed in options, the server will only be marked
    # unknown if the passed generation is no older than the current generation
    # of the server's connection pool.
    #
    # @param [ Hash ] options Options.
    #
    # @option options [ Integer ] :generation Connection pool generation of
    #   the connection that was used for the operation that produced the error.
    # @option options [ true | false ] :keep_connection_pool Usually when the
    #   new server description is unknown, the connection pool on the
    #   respective server is cleared. Set this option to true to keep the
    #   existing connection pool (required when handling not master errors
    #   on 4.2+ servers).
    # @option options [ TopologyVersion ] :topology_version Topology version
    #   of the error response that is causing the server to be marked unknown.
    # @option options [ true | false ] :stop_push_monitor Whether to stop
    #   the PushMonitor associated with the server, if any.
    # @option options [ Object ] :service_id Discard state for the specified
    #   service id only.
    #
    # @since 2.4.0, SDAM events are sent as of version 2.7.0
    def unknown!(options = {})
      pool = pool_internal

      if load_balancer?
        # When the client is in load-balanced topology, servers (the one and
        # only that can be) starts out as a load balancer and stays as a
        # load balancer indefinitely. As such it is not marked unknown.
        #
        # However, this method also clears connection pool for the server
        # when the latter is marked unknown, and this part needs to happen
        # when the server is a load balancer.
        #
        # It is possible for a load balancer server to not have a service id,
        # for example if there haven't been any successful connections yet to
        # this server, but the server can still be marked unknown if one
        # of such connections failed midway through its establishment.
        if service_id = options[:service_id]
          pool&.disconnect!(service_id: service_id)
        end
        return
      end

      if options[:generation] && options[:generation] < pool&.generation
        return
      end

      if options[:topology_version] && description.topology_version &&
        !options[:topology_version].gt?(description.topology_version)
      then
        return
      end

      if options[:stop_push_monitor]
        monitor&.stop_push_monitor!
      end

      # SDAM flow will update description on the server without in-place
      # mutations and invoke SDAM transitions as needed.
      config = {}
      if options[:service_id]
        config['serviceId'] = options[:service_id]
      end
      if options[:topology_version]
        config['topologyVersion'] = options[:topology_version]
      end
      new_description = Description.new(address, config,
        load_balancer: load_balancer?,
        force_load_balancer: options[:connect] == :load_balanced,
      )
      cluster.run_sdam_flow(description, new_description, options)
    end

    # @api private
    def update_description(description)
      pool = pool_internal
      if pool && !description.unknown?
        pool.ready
      end
      @description = description
    end

    # Clear the servers description so that it is considered unknown and can be
    # safely disconnected.
    #
    # @api private
    def clear_description
      @description = Mongo::Server::Description.new(address, {})
    end

    # @param [ Object ] :service_id Close connections with the specified
    #   service id only.
    # @param [ true | false ] :interrupt_in_use_connections Whether or not the
    #   cleared connections should be interrupted as well.
    #
    # @api private
    def clear_connection_pool(service_id: nil, interrupt_in_use_connections: false)
      @pool_lock.synchronize do
        # A server being marked unknown after it is closed is technically
        # incorrect but it does not meaningfully alter any state.
        # Because historically the driver permitted servers to be marked
        # unknown at any time, continue doing so even if the pool is closed.
        if @pool && !@pool.closed?
          @pool.disconnect!(service_id: service_id, interrupt_in_use_connections: interrupt_in_use_connections)
        end
      end
    end

    # @api private
    def next_connection_id
      @connection_id_gen.next_id
    end

    # @api private
    def update_last_scan
      @last_scan = Time.now
      @last_scan_monotime = Utils.monotonic_time
    end
  end
end

require 'mongo/server/app_metadata'
require 'mongo/server/connection_common'
require 'mongo/server/connection_base'
require 'mongo/server/pending_connection'
require 'mongo/server/connection'
require 'mongo/server/connection_pool'
require 'mongo/server/description'
require 'mongo/server/monitor'
require 'mongo/server/round_trip_time_averager'
require 'mongo/server/push_monitor'

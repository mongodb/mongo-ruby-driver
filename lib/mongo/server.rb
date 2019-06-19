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
    #
    # @since 2.0.0
    def initialize(address, cluster, monitoring, event_listeners, options = {})
      @address = address
      @cluster = cluster
      @monitoring = monitoring
      options = options.dup
      monitor = options.delete(:monitor)
      @options = options.freeze
      @event_listeners = event_listeners
      @connection_id_gen = Class.new do
        include Id
      end
      @monitor = Monitor.new(address, event_listeners, monitoring,
        options.merge(app_metadata: Monitor::AppMetadata.new(cluster.options)))
      unless monitor == false
        start_monitoring
      end
      @connected = true
      @pool_lock = Mutex.new
    end

    # @return [ String ] The configured address for the server.
    attr_reader :address

    # @return [ Cluster ] cluster The server cluster.
    attr_reader :cluster

    # @return [ Monitor ] monitor The server monitor.
    attr_reader :monitor

    # @return [ Hash ] The options hash.
    attr_reader :options

    # @return [ Monitoring ] monitoring The monitoring.
    attr_reader :monitoring

    # Get the description from the monitor and scan on monitor.
    def_delegators :monitor,
      :description,
      :scan!,
      :heartbeat_frequency,
      :last_scan,
      :compressor
    alias :heartbeat_frequency_seconds :heartbeat_frequency

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
                   :last_write_date,
                   :logical_session_timeout

    # Get the app metadata from the cluster.
    def_delegators :cluster,
                   :app_metadata,
                   :cluster_time,
                   :update_cluster_time

    def_delegators :features,
                   :check_driver_support!

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

    # Get a new context for this server in which to send messages.
    #
    # @example Get the server context.
    #   server.context
    #
    # @return [ Mongo::Server::Context ] context The server context.
    #
    # @since 2.0.0
    #
    # @deprecated Will be removed in version 3.0
    def context
      Context.new(self)
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

    # Disconnect the server from the connection.
    #
    # @example Disconnect the server.
    #   server.disconnect!
    #
    # @param [ Boolean ] wait Whether to wait for background threads to
    #   finish running.
    #
    # @return [ true ] Always true with no exception.
    #
    # @since 2.0.0
    def disconnect!(wait=false)
      begin
        # For backwards compatibility we disconnect/clear the pool rather
        # than close it here.
        pool.disconnect!
      rescue Error::PoolClosedError
        # If the pool was already closed, we don't need to do anything here.
      end
      monitor.stop!(wait)
      @connected = false
      true
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

    # When the server is flagged for garbage collection, stop the monitor
    # thread.
    #
    # @example Finalize the object.
    #   Server.finalize(monitor)
    #
    # @param [ Server::Monitor ] monitor The server monitor.
    #
    # @since 2.2.0
    def self.finalize(monitor)
      proc { monitor.stop! }
    end

    # Start monitoring the server.
    #
    # Used internally by the driver to add a server to a cluster
    # while delaying monitoring until the server is in the cluster.
    #
    # @api private
    def start_monitoring
      publish_sdam_event(
        Monitoring::SERVER_OPENING,
        Monitoring::Event::ServerOpening.new(address, cluster.topology)
      )
      if options[:monitoring_io] != false
        monitor.run!
        ObjectSpace.define_finalizer(self, self.class.finalize(monitor))
      end
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
      "#<Mongo::Server:0x#{object_id} address=#{address.host}:#{address.port}>"
    end

    # @note This method is experimental and subject to change.
    #
    # @api experimental
    # @since 2.7.0
    def summary
      status = case
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
        ''
      end
      if replica_set_name
        status += " replica_set=#{replica_set_name}"
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
      @pool_lock.synchronize do
        @pool ||= ConnectionPool.new(self, options)
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
    def with_connection(&block)
      pool.with_connection(&block)
    end

    # Handle handshake failure.
    #
    # @since 2.7.0
    # @api private
    def handle_handshake_failure!
      yield
    rescue Mongo::Error::SocketError, Mongo::Error::SocketTimeoutError
      unknown!
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
    rescue Mongo::Error::SocketError
      # non-timeout network error
      unknown!
      pool.disconnect!
      raise
    rescue Auth::Unauthorized
      # auth error, keep server description and topology as they are
      pool.disconnect!
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
    # @since 2.5.0
    def retry_writes?
      !!(features.sessions_enabled? && logical_session_timeout && !standalone?)
    end

    # Marks server unknown and publishes the associated SDAM event
    # (server description changed).
    #
    # @since 2.4.0, SDAM events are sent as of version 2.7.0
    def unknown!
      # Just dispatch the description changed event here, SDAM flow
      # will update description on the server without in-place mutations
      # and invoke SDAM transitions as needed.
      publish(Event::DESCRIPTION_CHANGED, description, Description.new(address))
    end

    # @api private
    def update_description(description)
      monitor.instance_variable_set('@description', description)
    end

    # @api private
    def next_connection_id
      @connection_id_gen.next_id
    end
  end
end

require 'mongo/server/app_metadata'
require 'mongo/server/connectable'
require 'mongo/server/connection_base'
require 'mongo/server/pending_connection'
require 'mongo/server/connection'
require 'mongo/server/connection_pool'
require 'mongo/server/context'
require 'mongo/server/description'
require 'mongo/server/monitor'
require 'mongo/server/round_trip_time_averager'

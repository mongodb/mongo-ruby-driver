# frozen_string_literal: true

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
  class Server
    # Responsible for periodically polling a server via hello commands to
    # keep the server's status up to date.
    #
    # Does all work in a background thread so as to not interfere with other
    # operations performed by the driver.
    #
    # @since 2.0.0
    # @api private
    class Monitor
      include Loggable
      extend Forwardable
      include Event::Publisher
      include BackgroundThread

      # The default interval between server status refreshes is 10 seconds.
      #
      # @since 2.0.0
      DEFAULT_HEARTBEAT_INTERVAL = 10

      # The minimum time between forced server scans. Is
      # minHeartbeatFrequencyMS in the SDAM spec.
      #
      # @since 2.0.0
      MIN_SCAN_INTERVAL = 0.5

      # The weighting factor (alpha) for calculating the average moving round trip time.
      #
      # @since 2.0.0
      # @deprecated Will be removed in version 3.0.
      RTT_WEIGHT_FACTOR = 0.2

      # Create the new server monitor.
      #
      # @example Create the server monitor.
      #   Mongo::Server::Monitor.new(address, listeners, monitoring)
      #
      # @note Monitor must never be directly instantiated outside of a Server.
      #
      # @param [ Server ] server The server to monitor.
      # @param [ Event::Listeners ] event_listeners The event listeners.
      # @param [ Monitoring ] monitoring The monitoring.
      # @param [ Hash ] options The options.
      #
      # @option options [ Float ] :connect_timeout The timeout, in seconds, to
      #   use when establishing the monitoring connection.
      # @option options [ Float ] :heartbeat_interval The interval between
      #   regular server checks.
      # @option options [ Logger ] :logger A custom logger to use.
      # @option options [ Mongo::Server::Monitor::AppMetadata ] :monitor_app_metadata
      #   The metadata to use for regular monitoring connection.
      # @option options [ Mongo::Server::Monitor::AppMetadata ] :push_monitor_app_metadata
      #   The metadata to use for push monitor's connection.
      # @option options [ Float ] :socket_timeout The timeout, in seconds, to
      #   execute operations on the monitoring connection.
      #
      # @since 2.0.0
      # @api private
      def initialize(server, event_listeners, monitoring, options = {})
        raise ArgumentError, "Wrong monitoring type: #{monitoring.inspect}" unless monitoring.is_a?(Monitoring)
        raise ArgumentError, 'App metadata is required' unless options[:app_metadata]
        raise ArgumentError, 'Push monitor app metadata is required' unless options[:push_monitor_app_metadata]

        @server = server
        @event_listeners = event_listeners
        @monitoring = monitoring
        @options = options.freeze
        @mutex = Mutex.new
        @sdam_mutex = Mutex.new
        @next_earliest_scan = @next_wanted_scan = Time.now
        @update_mutex = Mutex.new
        # Guards reads and writes of @connection so the polling connection can
        # be cancelled from another thread. Per the Server Monitoring spec's
        # cancelCheck pseudocode, the lock is only held long enough to copy or
        # assign the reference - never across the blocking check.
        @connection_lock = Mutex.new
      end

      # @return [ Server ] server The server that this monitor is monitoring.
      # @api private
      attr_reader :server

      # @return [ Mongo::Server::Monitor::Connection | nil ] The connection to
      #   use, read under @connection_lock so callers never observe a stale
      #   reference after a concurrent cancel_check! clears it.
      def connection
        @connection_lock.synchronize { @connection }
      end

      # @return [ Hash ] options The server options.
      attr_reader :options

      # The interval between regular server checks.
      #
      # @return [ Float ] The heartbeat interval, in seconds.
      def heartbeat_interval
        options[:heartbeat_interval] || DEFAULT_HEARTBEAT_INTERVAL
      end

      # @deprecated
      def_delegators :server, :last_scan

      # The compressor is determined during the handshake, so it must be an
      # attribute of the connection.
      #
      # @deprecated
      def_delegators :connection, :compressor

      # @return [ Monitoring ] monitoring The monitoring.
      attr_reader :monitoring

      # @return [ Server::PushMonitor | nil ] The push monitor, if one is being
      #   used.
      def push_monitor
        @update_mutex.synchronize do
          @push_monitor
        end
      end

      # Perform a check of the server.
      #
      # @since 2.0.0
      def do_work
        scan!
        # @next_wanted_scan may be updated by the push monitor.
        # However we need to check for termination flag so that the monitor
        # thread exits when requested.
        loop do
          delta = @next_wanted_scan - Time.now
          break unless delta > 0

          signaled = server.scan_semaphore.wait(delta)
          break if signaled || @stop_requested
        end
      end

      # Stop the background thread and wait for it to terminate for a
      # reasonable amount of time.
      #
      # @return [ true | false ] Whether the thread was terminated.
      #
      # @api public for backwards compatibility only
      def stop!
        stop_push_monitor!

        # Forward super's return value
        super.tap do
          # Important: disconnect should happen after the background thread
          # terminates.
          connection&.disconnect!
        end
      end

      # @param [ Mongo::Server::Monitor::Connection ] connection The freshly
      #   established monitoring connection. Passed explicitly (rather than read
      #   from @connection) so a concurrent cancel_check! cannot nil it out from
      #   under us between establishing it and building the PushMonitor.
      def create_push_monitor!(topology_version, connection)
        @update_mutex.synchronize do
          @push_monitor = nil if @push_monitor && !@push_monitor.running?

          @push_monitor ||= PushMonitor.new(
            self,
            topology_version,
            monitoring,
            **Utils.shallow_symbolize_keys(options.merge(
                                             socket_timeout: heartbeat_interval + connection.socket_timeout,
                                             app_metadata: options[:push_monitor_app_metadata],
                                             check_document: connection.check_document
                                           ))
          )
        end
      end

      def stop_push_monitor!
        @update_mutex.synchronize do
          if @push_monitor
            @push_monitor.stop!
            @push_monitor = nil
          end
        end
      end

      # Cancel the in-progress check and close the monitoring connection.
      #
      # Called when the server is marked Unknown from a network error, per the
      # Server Monitoring spec ("hello or legacy hello Cancellation"). Stops the
      # streaming PushMonitor (interrupting its awaited hello read) and closes
      # the polling connection, so the next check must establish a fresh one
      # rather than re-validating the server over a possibly-dead socket.
      #
      # @api private
      def cancel_check!
        stop_push_monitor!

        # Copy the connection reference under the lock, then interrupt and close
        # it outside the lock. Closing the socket interrupts any in-progress
        # read on the monitor thread; nil-ing the reference forces the next
        # check to reconnect. The monitor thread is the only writer of a new
        # connection, so it is safe for this thread to clear it.
        connection = @connection_lock.synchronize do
          conn = @connection
          @connection = nil
          conn
        end
        connection&.disconnect!
      end

      # Perform a check of the server with throttling, and update
      # the server's description and average round trip time.
      #
      # If the server was checked less than MIN_SCAN_INTERVAL seconds
      # ago, sleep until MIN_SCAN_INTERVAL seconds have passed since the last
      # check. Then perform the check which involves running hello
      # on the server being monitored and updating the server description
      # as a result.
      #
      # @note If the system clock moves backwards, this method can sleep
      #   for a very long time.
      #
      # @note The return value of this method is deprecated. In version 3.0.0
      #   this method will not have a return value.
      #
      # @return [ Description ] The updated description.
      #
      # @since 2.0.0
      def scan!
        # Ordinarily the background thread would invoke this method.
        # But it is also possible to invoke scan! directly on a monitor.
        # Allow only one scan to be performed at a time.
        @mutex.synchronize do
          throttle_scan_frequency!

          # When the streaming protocol is active the PushMonitor is the
          # authoritative SDAM source and this scan only measures RTT on the
          # dedicated connection. Per the Server Monitoring spec, an RTT
          # command MUST NOT publish events or update the topology. Compute
          # this before do_scan, which may (re)connect and change the state.
          rtt_only = rtt_measurement_only?

          begin
            result = do_scan(publish_heartbeat: !rtt_only)
          rescue StandardError => e
            run_sdam_flow({}, scan_error: e, rtt_only: rtt_only)
          else
            run_sdam_flow(result, rtt_only: rtt_only)
          end
        end
      end

      def run_sdam_flow(result, awaited: false, scan_error: nil, rtt_only: false)
        @sdam_mutex.synchronize do
          old_description = server.description

          # An RTT-only measurement (streaming protocol active) must not update
          # the topology or publish SDAM events. The RTT it gathered is
          # incorporated into the next streaming-hello description via the
          # shared RTT calculator. The scheduling below still runs so the
          # monitor keeps pacing its checks.
          unless rtt_only
            new_description = Description.new(
              server.address,
              result,
              average_round_trip_time: server.round_trip_time_calculator.average_round_trip_time,
              minimum_round_trip_time: server.round_trip_time_calculator.minimum_round_trip_time
            )
            server.cluster.run_sdam_flow(server.description, new_description, awaited: awaited, scan_error: scan_error)
          end

          server.description.tap do |new_description|
            unless awaited
              if new_description.unknown? && !old_description.unknown?
                @next_earliest_scan = @next_wanted_scan = Time.now
              else
                @next_earliest_scan = Time.now + MIN_SCAN_INTERVAL
                @next_wanted_scan = Time.now + heartbeat_interval
              end
            end
          end
        end
      end

      # Restarts the server monitor unless the current thread is alive.
      #
      # @example Restart the monitor.
      #   monitor.restart!
      #
      # @return [ Thread ] The thread the monitor runs on.
      #
      # @since 2.1.0
      def restart!
        if @thread && @thread.alive?
          @thread
        else
          run!
        end
      end

      def to_s
        "#<#{self.class.name}:#{object_id} #{server.address}>"
      end

      private

      def pre_stop
        server.scan_semaphore.signal
      end

      def do_scan(publish_heartbeat: true)
        if publish_heartbeat
          monitoring.publish_heartbeat(server) do
            check
          end
        else
          check
        end
      rescue StandardError => e
        msg = "Error checking #{server.address}"
        Utils.warn_bg_exception(msg, e,
                                logger: options[:logger],
                                log_prefix: options[:log_prefix],
                                bg_error_backtrace: options[:bg_error_backtrace])
        raise e
      end

      # Returns whether this scan is only an RTT measurement, which is the case
      # when the streaming protocol is active: a dedicated connection is already
      # established and the PushMonitor is running as the authoritative SDAM
      # source. In the polling protocol there is no running PushMonitor, so the
      # connection-reuse check is a real server check and not RTT-only.
      #
      # @return [ true | false ]
      def rtt_measurement_only?
        return false if connection.nil?

        # Only suppress the check while the server is in a known state and the
        # PushMonitor is the authoritative streaming source. If the server is
        # Unknown (e.g. an operation error or a streaming failure just marked
        # it so), the polling Monitor must run a full check to recover it
        # rather than waiting for the next streaming response - otherwise the
        # server can stay Unknown long enough to fail server selection.
        return false if server.unknown?

        pm = push_monitor
        !pm.nil? && pm.running?
      end

      def check
        # Snapshot the connection under the lock. A concurrent cancel_check!
        # may nil @connection from another thread; working on a local copy keeps
        # this check consistent, and the guarded writeback below never clobbers
        # a connection the monitor thread did not itself establish.
        connection = self.connection

        if connection && connection.pid != Process.pid
          log_warn("Detected PID change - Mongo client should have been reconnected (old pid #{connection.pid}, new pid #{Process.pid}")
          connection.disconnect!
          clear_connection(connection)
          connection = nil
        end

        if connection
          result = server.round_trip_time_calculator.measure do
            doc = connection.check_document
            cmd = Protocol::Query.new(
              Database::ADMIN, Database::COMMAND, doc, limit: -1
            )
            message = connection.dispatch_bytes(cmd.serialize.to_s)
            message.documents.first
          rescue Mongo::Error
            connection.disconnect!
            clear_connection(connection)
            raise
          end
        else
          connection = Connection.new(server.address, options)
          connection.connect!
          result = server.round_trip_time_calculator.measure do
            connection.handshake!
          end
          store_connection(connection)
          if (tv_doc = result['topologyVersion'])
            if streaming_enabled?
              # Run the instance we just created rather than re-reading the
              # push_monitor getter: a concurrent cancel_check! may have nil'd
              # @push_monitor between the two calls.
              push_monitor = create_push_monitor!(TopologyVersion.new(tv_doc), connection)
              push_monitor.run!
            else
              stop_push_monitor!
            end
          else
            # Failed response or pre-4.4 server
            stop_push_monitor!
          end
          result
        end
        result
      end

      # Store a freshly established monitoring connection.
      def store_connection(connection)
        @connection_lock.synchronize do
          @connection = connection
        end
      end

      # Clear the monitoring connection, but only if it is still the one passed
      # in. A concurrent cancel_check! may have already cleared or replaced it,
      # in which case we must leave the current connection alone.
      def clear_connection(connection)
        @connection_lock.synchronize do
          @connection = nil if @connection.equal?(connection)
        end
      end

      # @note If the system clock is set to a time in the past, this method
      #   can sleep for a very long time.
      def throttle_scan_frequency!
        delta = @next_earliest_scan - Time.now
        return unless delta > 0

        sleep(delta)
      end

      # Returns whether the streaming protocol is enabled, based on the
      # serverMonitoringMode option. Default mode is :auto.
      #
      # - :stream - always use streaming when server supports it
      # - :poll - never use streaming
      # - :auto - use polling on FaaS platforms, streaming otherwise
      #
      # @return [ true | false ] Whether streaming is enabled.
      def streaming_enabled?
        mode = options[:server_monitoring_mode] || :auto
        case mode
        when :poll
          false
        when :stream
          true
        when :auto
          !Server::AppMetadata::Environment.new.faas?
        end
      end
    end
  end
end

require 'mongo/server/monitor/connection'
require 'mongo/server/monitor/app_metadata'

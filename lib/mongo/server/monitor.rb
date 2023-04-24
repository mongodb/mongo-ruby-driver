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
      DEFAULT_HEARTBEAT_INTERVAL = 10.freeze

      # The minimum time between forced server scans. Is
      # minHeartbeatFrequencyMS in the SDAM spec.
      #
      # @since 2.0.0
      MIN_SCAN_INTERVAL = 0.5.freeze

      # The weighting factor (alpha) for calculating the average moving round trip time.
      #
      # @since 2.0.0
      # @deprecated Will be removed in version 3.0.
      RTT_WEIGHT_FACTOR = 0.2.freeze

      # Create the new server monitor.
      #
      # @example Create the server monitor.
      #   Mongo::Server::Monitor.new(address, listeners, monitoring)
      #
      # @note Monitor must never be directly instantiated outside of a Server.
      #
      # @param [ Server ] server The server to monitor.
      # @param [ Event::Listeners ] event_listeners The event listeners.
      # @param [ Monitoring ] monitoring The monitoring..
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
        unless monitoring.is_a?(Monitoring)
          raise ArgumentError, "Wrong monitoring type: #{monitoring.inspect}"
        end
        unless options[:app_metadata]
          raise ArgumentError, 'App metadata is required'
        end
        unless options[:push_monitor_app_metadata]
          raise ArgumentError, 'Push monitor app metadata is required'
        end
        @server = server
        @event_listeners = event_listeners
        @monitoring = monitoring
        @options = options.freeze
        @mutex = Mutex.new
        @sdam_mutex = Mutex.new
        @next_earliest_scan = @next_wanted_scan = Time.now
        @update_mutex = Mutex.new
      end

      # @return [ Server ] server The server that this monitor is monitoring.
      # @api private
      attr_reader :server

      # @return [ Mongo::Server::Monitor::Connection ] connection The connection to use.
      attr_reader :connection

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
          if delta > 0
            signaled = server.scan_semaphore.wait(delta)
            if signaled || @stop_requested
              break
            end
          else
            break
          end
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

      def create_push_monitor!(topology_version)
        @update_mutex.synchronize do
          if @push_monitor && !@push_monitor.running?
            @push_monitor = nil
          end

          @push_monitor ||= PushMonitor.new(
            self,
            topology_version,
            monitoring,
            **Utils.shallow_symbolize_keys(options.merge(
              socket_timeout: heartbeat_interval + connection.socket_timeout,
              app_metadata: options[:push_monitor_app_metadata],
              check_document: @connection.check_document
            )),
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

          begin
            result = do_scan
          rescue => e
            run_sdam_flow({}, scan_error: e)
          else
            run_sdam_flow(result)
          end
        end
      end

      def run_sdam_flow(result, awaited: false, scan_error: nil)
        @sdam_mutex.synchronize do
          old_description = server.description

          new_description = Description.new(server.address, result,
            average_round_trip_time: server.round_trip_time_averager.average_round_trip_time
          )

          server.cluster.run_sdam_flow(server.description, new_description, awaited: awaited, scan_error: scan_error)

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

      def do_scan
        begin
          monitoring.publish_heartbeat(server) do
            check
          end
        rescue => exc
          msg = "Error checking #{server.address}"
          Utils.warn_bg_exception(msg, exc,
            logger: options[:logger],
            log_prefix: options[:log_prefix],
            bg_error_backtrace: options[:bg_error_backtrace],
          )
          raise exc
        end
      end

      def check
        if @connection && @connection.pid != Process.pid
          log_warn("Detected PID change - Mongo client should have been reconnected (old pid #{@connection.pid}, new pid #{Process.pid}")
          @connection.disconnect!
          @connection = nil
        end

        if @connection
          result = server.round_trip_time_averager.measure do
            begin
              doc = @connection.check_document
              cmd = Protocol::Query.new(
                Database::ADMIN, Database::COMMAND, doc, :limit => -1
              )
              message = @connection.dispatch_bytes(cmd.serialize.to_s)
              message.documents.first
            rescue Mongo::Error
              @connection.disconnect!
              @connection = nil
              raise
            end
          end
        else
          connection = Connection.new(server.address, options)
          connection.connect!
          result = server.round_trip_time_averager.measure do
            connection.handshake!
          end
          @connection = connection
          if tv_doc = result['topologyVersion']
            # Successful response, server 4.4+
            create_push_monitor!(TopologyVersion.new(tv_doc))
            push_monitor.run!
          else
            # Failed response or pre-4.4 server
            stop_push_monitor!
          end
          result
        end
        result
      end

      # @note If the system clock is set to a time in the past, this method
      #   can sleep for a very long time.
      def throttle_scan_frequency!
        delta = @next_earliest_scan - Time.now
        if delta > 0
          sleep(delta)
        end
      end
    end
  end
end

require 'mongo/server/monitor/connection'
require 'mongo/server/monitor/app_metadata'

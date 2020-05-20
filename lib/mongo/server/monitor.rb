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

    # Responsible for periodically polling a server via ismaster commands to
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
      # @option options [ Float ] :socket_timeout The timeout, in seconds, to
      #   execute operations on the monitoring connection.
      #
      # @since 2.0.0
      # @api private
      def initialize(server, event_listeners, monitoring, options = {})
        unless monitoring.is_a?(Monitoring)
          raise ArgumentError, "Wrong monitoring type: #{monitoring.inspect}"
        end
        @server = server
        @event_listeners = event_listeners
        @monitoring = monitoring
        @options = options.freeze
        # This is a Mongo::Server::Monitor::Connection
        @connection = Connection.new(server.address, options)
        @mutex = Mutex.new
        @scan_started_at = nil
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

      # The compressor is determined during the handshake, so it must be an attribute
      # of the connection.
      #
      # @deprecated
      def_delegators :connection, :compressor

      # @return [ Monitoring ] monitoring The monitoring.
      attr_reader :monitoring

      # Runs the server monitor. Refreshing happens on a separate thread per
      # server.
      #
      # @example Run the monitor.
      #   monitor.run
      #
      # @return [ Thread ] The thread the monitor runs on.
      #
      # @since 2.0.0
      def do_work
        scan!
        server.scan_semaphore.wait(heartbeat_interval)
      end

      # Stop the background thread and wait for to terminate for a reasonable
      # amount of time.
      #
      # @return [ true | false ] Whether the thread was terminated.
      #
      # @api public for backwards compatibility only
      def stop!
        # Forward super's return value
        super.tap do
          # Important: disconnect should happen after the background thread
          # terminates.
          connection.disconnect!
        end
      end

      # Perform a check of the server with throttling, and update
      # the server's description and average round trip time.
      #
      # If the server was checked less than MIN_SCAN_INTERVAL seconds
      # ago, sleep until MIN_SCAN_INTERVAL seconds have passed since the last
      # check. Then perform the check which involves running isMaster
      # on the server being monitored and updating the server description
      # as a result.
      #
      # If the server check fails for any reason (such as a network error),
      # the check is retried by this method.
      #
      # If the server check fails twice, this method updates the server
      # description accordingly but does not raise an exception.
      #
      # @note If the system clock moves backwards, this method can sleep
      #   for a very long time.
      #
      # @note The return value of this method is deprecated. In version 3.0.0
      #   this method will not have a return value.
      #
      # @example Run a scan.
      #   monitor.scan!
      #
      # @return [ Description ] The updated description.
      #
      # @since 2.0.0
      def scan!
        throttle_scan_frequency!
        result = ismaster
        new_description = Description.new(server.address, result,
          server.round_trip_time_averager.average_round_trip_time)
        server.cluster.run_sdam_flow(server.description, new_description)
        server.description
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

      private

      def pre_stop
        server.scan_semaphore.signal
      end

      def ismaster
        @mutex.synchronize do
          if monitoring.monitoring?
            monitoring.started(
              Monitoring::SERVER_HEARTBEAT,
              Monitoring::Event::ServerHeartbeatStarted.new(server.address)
            )
          end

          # The duration we publish in heartbeat succeeded/failed events is
          # the time spent on the entire heartbeat. This could include time
          # to connect the socket (including TLS handshake), not just time
          # spent on ismaster call itself.
          # The spec at https://github.com/mongodb/specifications/blob/master/source/server-discovery-and-monitoring/server-discovery-and-monitoring-monitoring.rst
          # requires that the duration exposed here start from "sending the
          # message" (ismaster). This requirement does not make sense if,
          # for example, we were never able to connect to the server at all
          # and thus ismaster was never sent.
          start_time = Time.now

          begin
            result = server.round_trip_time_averager.measure do
              connection.ismaster
            end
          rescue => exc
            log_debug("Error running ismaster on #{server.address}: #{exc.class}: #{exc}:\n#{exc.backtrace[0..5].join("\n")}")
            if monitoring.monitoring?
              monitoring.failed(
                Monitoring::SERVER_HEARTBEAT,
                Monitoring::Event::ServerHeartbeatFailed.new(server.address, Time.now-start_time, exc)
              )
            end
            result = {}
          else
            if monitoring.monitoring?
              monitoring.succeeded(
                Monitoring::SERVER_HEARTBEAT,
                Monitoring::Event::ServerHeartbeatSucceeded.new(server.address, Time.now-start_time)
              )
            end
          end
          result
        end
      end

      # @note If the system clock is set to a time in the past, this method
      #   can sleep for a very long time.
      def throttle_scan_frequency!
        # Normally server.last_scan indicates when the previous scan
        # completed, but if scan! is manually invoked repeatedly then
        # server.last_scan won't be updated and multiple scans with no
        # cooldown can be obtained. Guard against repeated direct scan!
        # invocation also.
        last_time = [server.last_scan, @scan_started_at].compact.max
        if last_time
          difference = (Time.now - last_time)
          throttle_time = (MIN_SCAN_INTERVAL - difference)
          sleep(throttle_time) if throttle_time > 0
        end
        @scan_started_at = Time.now
      end
    end
  end
end

require 'mongo/server/monitor/connection'
require 'mongo/server/monitor/app_metadata'

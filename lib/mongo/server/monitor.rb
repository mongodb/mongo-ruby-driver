# Copyright (C) 2014-2019 MongoDB Inc.
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
    class Monitor
      include Loggable
      extend Forwardable
      include Event::Publisher

      # The default time for a server to refresh its status is 10 seconds.
      #
      # @since 2.0.0
      HEARTBEAT_FREQUENCY = 10.freeze

      # The minimum time between forced server scans. Is
      # minHeartbeatFrequencyMS in the SDAM spec.
      #
      # @since 2.0.0
      MIN_SCAN_FREQUENCY = 0.5.freeze

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
      # @option options [ Float ] :heartbeat_frequency The interval, in seconds,
      #   between server description refreshes via ismaster.
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
        @last_scan = nil
        @mutex = Mutex.new
      end

      # @return [ Server ] server The server that this monitor is monitoring.
      # @api private
      attr_reader :server

      # @return [ Mongo::Server::Monitor::Connection ] connection The connection to use.
      attr_reader :connection

      # @return [ Hash ] options The server options.
      attr_reader :options

      # @return [ Time ] last_scan The time when the last server scan started.
      #
      # @since 2.4.0
      attr_reader :last_scan

      # The compressor is determined during the handshake, so it must be an attribute
      # of the connection.
      def_delegators :connection, :compressor

      # @return [ Monitoring ] monitoring The monitoring.
      attr_reader :monitoring

      # Get the refresh interval for the server. This will be defined via an
      # option or will default to 10.
      #
      # @example Get the refresh interval.
      #   server.heartbeat_frequency
      #
      # @return [ Integer ] The heartbeat frequency, in seconds.
      #
      # @since 2.0.0
      def heartbeat_frequency
        @heartbeat_frequency ||= options[:heartbeat_frequency] || HEARTBEAT_FREQUENCY
      end

      # Runs the server monitor. Refreshing happens on a separate thread per
      # server.
      #
      # @example Run the monitor.
      #   monitor.run
      #
      # @return [ Thread ] The thread the monitor runs on.
      #
      # @since 2.0.0
      def run!
        @thread = Thread.new(heartbeat_frequency) do |i|
          loop do
            scan!
            server.scan_semaphore.wait(i)
          end
        end
      end

      # Perform a check of the server with throttling, and update
      # the server's description and average round trip time.
      #
      # If the server was checked less than MIN_SCAN_FREQUENCY seconds
      # ago, sleep until MIN_SCAN_FREQUENCY seconds have passed since the last
      # check. Then perform the check which involves running isMaster
      # on the server being monitored and updating the server description
      # as a result.
      #
      # @note If the system clock is set to a time in the past, this method
      #   can sleep for a very long time.
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

      # Stops the server monitor. Kills the thread so it doesn't continue
      # taking memory and sending commands to the connection.
      #
      # @example Stop the monitor.
      #   monitor.stop!
      #
      # @param [ Boolean ] wait Whether to wait for background threads to
      #   finish running.
      #
      # @return [ Boolean ] Is the thread stopped?
      #
      # @since 2.0.0
      def stop!(wait=false)
        # Although disconnect! documentation implies a possibility of
        # failure, all of our disconnects always return true
        if connection.disconnect!
          if @thread
            @thread.kill
            if wait
              @thread.join
              @thread = nil
              true
            else
              !@thread.alive?
            end
          else
            true
          end
        else
          false
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

      private

      def ismaster
        @mutex.synchronize do
          if monitoring.monitoring?
            monitoring.started(
              Monitoring::SERVER_HEARTBEAT,
              Monitoring::Event::ServerHeartbeatStarted.new(server.address)
            )
          end

          result, exc, rtt, average_rtt = server.round_trip_time_averager.measure do
            connection.ismaster
          end
          if exc
            log_debug("Error running ismaster on #{server.address}: #{exc.message}")
            if monitoring.monitoring?
              monitoring.failed(
                Monitoring::SERVER_HEARTBEAT,
                Monitoring::Event::ServerHeartbeatFailed.new(server.address, rtt, exc)
              )
            end
            result = {}
          else
            if monitoring.monitoring?
              monitoring.succeeded(
                Monitoring::SERVER_HEARTBEAT,
                Monitoring::Event::ServerHeartbeatSucceeded.new(server.address, rtt)
              )
            end
          end
          result
        end
      end

      # @note If the system clock is set to a time in the past, this method
      #   can sleep for a very long time.
      def throttle_scan_frequency!
        if @last_scan
          difference = (Time.now - @last_scan)
          throttle_time = (MIN_SCAN_FREQUENCY - difference)
          sleep(throttle_time) if throttle_time > 0
        end
        @last_scan = Time.now
      end
    end
  end
end

require 'mongo/server/monitor/connection'
require 'mongo/server/monitor/app_metadata'

# Copyright (C) 2014-2018 MongoDB Inc.
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

    # This object is responsible for keeping server status up to date, running in
    # a separate thread as to not disrupt other operations.
    #
    # @since 2.0.0
    class Monitor
      include Loggable
      extend Forwardable

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
      RTT_WEIGHT_FACTOR = 0.2.freeze

      # Create the new server monitor.
      #
      # @example Create the server monitor.
      #   Mongo::Server::Monitor.new(address, listeners, monitoring)
      #
      # @note Monitor must never be directly instantiated outside of a Server.
      #
      # @param [ Address ] address The address to monitor.
      # @param [ Event::Listeners ] listeners The event listeners.
      # @param [ Monitoring ] monitoring The monitoring..
      # @param [ Hash ] options The options.
      # @option options [ Float ] :heartbeat_frequency The interval, in seconds,
      #   between server description refreshes via ismaster.
      #
      # @since 2.0.0
      # @api private
      def initialize(address, listeners, monitoring, options = {})
        unless monitoring.is_a?(Monitoring)
          raise ArgumentError, "Wrong monitoring type: #{monitoring.inspect}"
        end
        @description = Description.new(address, {})
        @inspector = Description::Inspector.new(listeners)
        @monitoring = monitoring
        @options = options.freeze
        # This is a Mongo::Server::Monitor::Connection
        @connection = Connection.new(address, options)
        @average_round_trip_time = nil
        @last_scan = nil
        @mutex = Mutex.new
      end

      # @return [ Mongo::Server::Monitor::Connection ] connection The connection to use.
      attr_reader :connection

      # @return [ Server::Description ] description The server
      #   description the monitor refreshes.
      attr_reader :description

      # @return [ Description::Inspector ] inspector The description inspector.
      attr_reader :inspector

      # @return [ Hash ] options The server options.
      attr_reader :options

      # @return [ Time ] last_scan The time of the last server scan.
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
            sleep(i)
            scan!
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
      # @example Run a scan.
      #   monitor.scan!
      #
      # @return [ Description ] The updated description.
      #
      # @since 2.0.0
      def scan!
        throttle_scan_frequency!
        # ismaster call updates @average_round_trip_time
        result = ismaster
        @description = inspector.run(description, result, @average_round_trip_time)
        @description
      end

      # Sets server description to unknown.
      #
      # @since 2.7.0
      # @api private
      def unknown!
        @description = Description.new(description.address)
      end

      # Stops the server monitor. Kills the thread so it doesn't continue
      # taking memory and sending commands to the connection.
      #
      # @example Stop the monitor.
      #   monitor.stop!
      #
      # @return [ Boolean ] Is the thread stopped?
      #
      # @since 2.0.0
      def stop!
        # Although disconnect! documentation implies a possibility of
        # failure, all of our disconnects always return true
        if connection.disconnect!
          if @thread
            @thread.kill && @thread.stop?
          else
            true
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
        @thread.alive? ? @thread : run!
      end

      private

      def round_trip_time(start)
        Time.now - start
      end

      def round_trip_times(start)
        new_rtt = round_trip_time(start)
        average_rtt = if @average_round_trip_time
          RTT_WEIGHT_FACTOR * new_rtt + (1 - RTT_WEIGHT_FACTOR) * @average_round_trip_time
        else
          new_rtt
        end
        [new_rtt, average_rtt]
      end

      def ismaster
        @mutex.synchronize do
          start = Time.now
          monitoring.started(
            Monitoring::SERVER_HEARTBEAT,
            Monitoring::Event::ServerHeartbeatStarted.new(connection.address)
          )

          begin
            result = connection.ismaster
          rescue Exception => e
            rtt, @average_round_trip_time = round_trip_times(start)
            log_debug("Error running ismaster on #{connection.address}: #{e.message}")
            monitoring.failed(
              Monitoring::SERVER_HEARTBEAT,
              Monitoring::Event::ServerHeartbeatFailed.new(connection.address, rtt, e)
            )
            result = {}
          else
            rtt, @average_round_trip_time = round_trip_times(start)
            monitoring.succeeded(
              Monitoring::SERVER_HEARTBEAT,
              Monitoring::Event::ServerHeartbeatSucceeded.new(connection.address, rtt)
            )
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

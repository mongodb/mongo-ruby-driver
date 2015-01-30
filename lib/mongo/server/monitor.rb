# Copyright (C) 2014-2015 MongoDB Inc.
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

require 'weakref'
require 'mongo/server/monitor/connection'

module Mongo
  class Server

    # This object is responsible for keeping server status up to date, running in
    # a separate thread as to not disrupt other operations.
    #
    # @since 2.0.0
    class Monitor
      include Loggable

      # The default time for a server to refresh its status is 10 seconds.
      #
      # @since 2.0.0
      HEARTBEAT_FREQUENCY = 10.freeze

      # The command used for determining server status.
      #
      # @since 2.0.0
      STATUS = { :ismaster => 1 }.freeze

      # The constant for the ismaster command.
      #
      # @since 2.0.0
      ISMASTER = Protocol::Query.new(Database::ADMIN, Database::COMMAND, STATUS, :limit => -1)

      # @return [ Mongo::Server::Description ] description The server
      #   description the monitor refreshes.
      attr_reader :description

      # @return [ Hash ] options The server options.
      attr_reader :options

      # @return [ Mongo::Connection ] connection The connection to use.
      attr_reader :connection

      # Force the monitor to immediately do a check of its server.
      #
      # @example Force a scan.
      #   monitor.scan!
      #
      # @return [ Description ] The updated description.
      #
      # @since 2.0.0
      def scan!
        description.update!(*ismaster)
      end

      # Get the refresh interval for the server. This will be defined via an option
      # or will default to 5.
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

      # Create the new server monitor.
      #
      # @example Create the server monitor.
      #   Mongo::Server::Monitor.new(address, description)
      #
      # @param [ Server::Description ] server The server description to refresh.
      # @param [ Integer ] interval The refresh interval in seconds.
      #
      # @since 2.0.0
      def initialize(description, options = {})
        @description = WeakRef.new(description)
        @options = options.freeze
        @connection = Connection.new(description.address, options)
      end

      # Runs the server monitor. Refreshing happens on a separate thread per
      # server.
      #
      # @example Run the monitor.
      #   monito.run
      #
      # @return [ Thread ] The thread the monitor runs on.
      #
      # @since 2.0.0
      def run!
        @thread = Thread.new(heartbeat_frequency) do |i|
          loop do
            sleep(i)
            if description.weakref_alive?
              check!
            else
              stop!
            end
          end
        end
      end

      # Stops the server monitor. Kills the thread so it doesn't continue
      # taking memory and sending commands to the connection.
      #
      # @example Stop the monitor.
      #   monitor.stop!
      #
      # @return [ Boolean ] Is the Thread stopped?
      #
      # @since 2.0.0
      def stop!
        @thread.kill && @thread.stop?
      end

      private

      def calculate_round_trip_time(start)
        Time.now - start
      end

      def ismaster
        start = Time.now
        begin
          result = connection.dispatch([ ISMASTER ]).documents[0]
          return result, calculate_round_trip_time(start)
        rescue Exception => e
          log_debug([ e.message ])
          connection.disconnect!
          return {}, calculate_round_trip_time(start)
        end
      end
    end
  end
end

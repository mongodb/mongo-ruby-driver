# Copyright (C) 2009 - 2014 MongoDB Inc.
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

      # The default time for a server to refresh its status is 5 seconds.
      #
      # @since 2.0.0
      HEARTBEAT_FREQUENCY = 5.freeze

      # The command used for determining server status.
      #
      # @since 2.0.0
      STATUS = { :ismaster => 1 }.freeze

      # @return [ Mutex ] The refresh operation mutex.
      attr_reader :mutex
      # @return [ Mongo::Server ] The server the monitor refreshes.
      attr_reader :server
      # @return [ Hash ] options The server options.
      attr_reader :options
      # @return [ Mongo::Connection ] connection The connection to use.
      attr_reader :connection

      # Check the server status immediately.
      #
      # @example Check the server status.
      #   monitor.check!
      #
      # @return [ Server::Description ] The updated server description.
      #
      # @since 2.0.0
      def check!
        server.description.update!(*ismaster)
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
      #   Mongo::Server::Monitor.new(server, 5)
      #
      # @param [ Mongo::Server ] server The server to refresh.
      # @param [ Integer ] interval The refresh interval in seconds.
      #
      # @since 2.0.0
      def initialize(server, options = {})
        @server = server
        @options = options
        @mutex = Mutex.new
        @connection = Mongo::Pool::Connection.new(server.address, options[:timeout], options)
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
      def run
        Thread.new(heartbeat_frequency, server) do |i, s|
          loop do
            sleep(i)
            check!
          end
        end
      end

      private

      def calculate_round_trip_time(start)
        Time.now - start
      end

      def ismaster
        mutex.synchronize do
          start = Time.now
          begin
            connection.write([ ismaster_command ])
            result = connection.read.documents[0]
            return result, calculate_round_trip_time(start)
          rescue SystemCallError, IOError
            return {}, calculate_round_trip_time(start)
          end
        end
      end

      def ismaster_command
        Protocol::Query.new(Database::ADMIN, Database::COMMAND, STATUS, :limit => -1)
      end
    end
  end
end

# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2020 MongoDB Inc.
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

    # A monitor utilizing server-pushed hello requests.
    #
    # When a Monitor handshakes with a 4.4+ server, it creates an instance
    # of PushMonitor. PushMonitor subsequently executes server-pushed hello
    # (i.e. awaited & exhausted hello) to receive topology changes from the
    # server as quickly as possible. The Monitor still monitors the server
    # for round-trip time calculations and to perform immediate checks as
    # requested by the application.
    #
    # @api private
    class PushMonitor
      extend Forwardable
      include BackgroundThread

      def initialize(monitor, topology_version, monitoring, **options)
        if topology_version.nil?
          raise ArgumentError, 'Topology version must be provided but it was nil'
        end
        unless options[:app_metadata]
          raise ArgumentError, 'App metadata is required'
        end
        unless options[:check_document]
          raise ArgumentError, 'Check document is required'
        end
        @app_metadata = options[:app_metadata]
        @check_document = options[:check_document]
        @monitor = monitor
        @topology_version = topology_version
        @monitoring = monitoring
        @options = options
        @lock = Mutex.new
      end

      # @return [ Monitor ] The monitor to which this push monitor is attached.
      attr_reader :monitor

      # @return [ TopologyVersion ] Most recently received topology version.
      attr_reader :topology_version

      # @return [ Monitoring ] monitoring The monitoring.
      attr_reader :monitoring

      # @return [ Hash ] Push monitor options.
      attr_reader :options

      # @return [ Server ] The server that is being monitored.
      def_delegator :monitor, :server

      def start!
        @lock.synchronize do
          super
        end
      end

      def stop!
        @lock.synchronize do
          @stop_requested = true
          if @connection
            # Interrupt any in-progress exhausted hello reads by
            # disconnecting the connection.
            @connection.send(:socket).close rescue nil
          end
        end
        super.tap do
          @lock.synchronize do
            if @connection
              @connection.disconnect!
              @connection = nil
            end
          end
        end
      end

      def do_work
        @lock.synchronize do
          return if @stop_requested
        end

        result = monitoring.publish_heartbeat(server, awaited: true) do
          check
        end
        new_description = monitor.run_sdam_flow(result, awaited: true)
        # When hello fails due to a fail point, the response does not
        # include topology version. In this case we need to keep our existing
        # topology version so that we can resume monitoring.
        # The spec does not appear to directly address this case but
        # https://github.com/mongodb/specifications/blob/master/source/server-discovery-and-monitoring/server-monitoring.rst#streaming-ismaster
        # says that topologyVersion should only be updated from successful
        # hello responses.
        if new_description.topology_version
          @topology_version = new_description.topology_version
        end
      rescue IOError, SocketError, SystemCallError, Mongo::Error => exc
        stop_requested = @lock.synchronize { @stop_requested }
        if stop_requested
          # Ignore the exception, see RUBY-2771.
          return
        end

        msg = "Error running awaited hello on #{server.address}"
        Utils.warn_bg_exception(msg, exc,
          logger: options[:logger],
          log_prefix: options[:log_prefix],
          bg_error_backtrace: options[:bg_error_backtrace],
        )

        # If a request failed on a connection, stop push monitoring.
        # In case the server is dead we don't want to have two connections
        # trying to connect unsuccessfully at the same time.
        stop!

        # Request an immediate check on the monitor to get reinstated as
        # soon as possible in case the server is actually alive.
        server.scan_semaphore.signal
      end

      def check
        @lock.synchronize do
          if @connection && @connection.pid != Process.pid
            log_warn("Detected PID change - Mongo client should have been reconnected (old pid #{@connection.pid}, new pid #{Process.pid}")
            @connection.disconnect!
            @connection = nil
          end
        end

        @lock.synchronize do
          unless @connection
            @server_pushing = false
            connection = PushMonitor::Connection.new(server.address, options)
            connection.connect!
            @connection = connection
          end
        end

        resp_msg = begin
          unless @server_pushing
            write_check_command
          end
          read_response
        rescue Mongo::Error
          @lock.synchronize do
            @connection.disconnect!
            @connection = nil
          end
          raise
        end
        @server_pushing = resp_msg.flags.include?(:more_to_come)
        result = Operation::Result.new(resp_msg)
        result.validate!
        result.documents.first
      end

      def write_check_command
        document = @check_document.merge(
          topologyVersion: topology_version.to_doc,
          maxAwaitTimeMS: monitor.heartbeat_interval * 1000,
        )
        command = Protocol::Msg.new(
          [:exhaust_allowed], {}, document.merge({'$db' => Database::ADMIN})
        )
        @lock.synchronize { @connection }.write_bytes(command.serialize.to_s)
      end

      def read_response
        if timeout = options[:connect_timeout]
          if timeout < 0
            raise Mongo::SocketTimeoutError, "Requested to read with a negative timeout: #{}"
          elsif timeout > 0
            timeout += options[:heartbeat_frequency] || Monitor::DEFAULT_HEARTBEAT_INTERVAL
          end
        end
        # We set the timeout twice: once passed into read_socket which applies
        # to each individual read operation, and again around the entire read.
        Timeout.timeout(timeout, Error::SocketTimeoutError, "Failed to read an awaited hello response in #{timeout} seconds") do
          @lock.synchronize { @connection }.read_response(socket_timeout: timeout)
        end
      end

      def to_s
        "#<#{self.class.name}:#{object_id} #{server.address}>"
      end

    end
  end
end

require 'mongo/server/push_monitor/connection'

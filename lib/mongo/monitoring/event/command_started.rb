# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2015-2020 MongoDB Inc.
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

module Mongo
  class Monitoring
    module Event

      # Event that is fired when a command operation starts.
      #
      # @since 2.1.0
      class CommandStarted < Mongo::Event::Base
        include Secure

        # @return [ Server::Address ] address The server address.
        attr_reader :address

        # @return [ BSON::Document ] command The command arguments.
        attr_reader :command

        # @return [ String ] command_name The name of the command.
        attr_reader :command_name

        # @return [ String ] database_name The name of the database_name.
        attr_reader :database_name

        # @return [ Integer ] operation_id The operation id.
        attr_reader :operation_id

        # @return [ Integer ] request_id The request id.
        attr_reader :request_id

        # @return [ nil | Object ] The service id, if any.
        attr_reader :service_id

        # object_id of the socket object used for this command.
        #
        # @api private
        attr_reader :socket_object_id

        # @api private
        attr_reader :connection_generation

        # @return [ Integer ] The ID for the connection over which the command
        #   is sent.
        #
        # @api private
        attr_reader :connection_id

        # @return [ Integer ] server_connection_id The server connection id.
        attr_reader :server_connection_id

        # @return [ true | false ] Whether the event contains sensitive data.
        #
        # @api private
        attr_reader :sensitive

        # Create the new event.
        #
        # @example Create the event.
        #
        # @param [ String ] command_name The name of the command.
        # @param [ String ] database_name The database_name name.
        # @param [ Server::Address ] address The server address.
        # @param [ Integer ] request_id The request id.
        # @param [ Integer ] operation_id The operation id.
        # @param [ BSON::Document ] command The command arguments.
        # @param [ Object ] service_id The service id, if any.
        #
        # @since 2.1.0
        # @api private
        def initialize(command_name, database_name, address, request_id,
          operation_id, command, socket_object_id: nil, connection_id: nil,
          connection_generation: nil, server_connection_id: nil,
          service_id: nil
        )
          @command_name = command_name.to_s
          @database_name = database_name
          @address = address
          @request_id = request_id
          @operation_id = operation_id
          @service_id = service_id
          @sensitive = sensitive?(
            command_name: @command_name,
            document: command
          )
          @command = redacted(command_name, command)
          @socket_object_id = socket_object_id
          @connection_id = connection_id
          @connection_generation = connection_generation
          @server_connection_id = server_connection_id
        end

        # Returns a concise yet useful summary of the event.
        #
        # @return [ String ] String summary of the event.
        #
        # @note This method is experimental and subject to change.
        #
        # @api experimental
        def summary
          "#<#{short_class_name} address=#{address} #{database_name}.#{command_name} command=#{command_summary}>"
        end

        # Returns the command, formatted as a string, with automatically added
        # keys elided ($clusterTime, lsid, signature).
        #
        # @return [ String ] The command summary.
        private def command_summary
          command = self.command
          remove_keys = %w($clusterTime lsid signature)
          if remove_keys.any? { |k| command.key?(k) }
            command = Hash[command.reject { |k, v| remove_keys.include?(k) }]
            suffix = ' ...'
          else
            suffix = ''
          end
          command.map do |k, v|
            "#{k}=#{v.inspect}"
          end.join(' ') + suffix
        end

        # Create the event from a wire protocol message payload.
        #
        # @example Create the event.
        #   CommandStarted.generate(address, 1, payload)
        #
        # @param [ Server::Address ] address The server address.
        # @param [ Integer ] operation_id The operation id.
        # @param [ Hash ] payload The message payload.
        # @param [ Object ] service_id The service id, if any.
        #
        # @return [ CommandStarted ] The event.
        #
        # @since 2.1.0
        # @api private
        def self.generate(address, operation_id, payload,
          socket_object_id: nil, connection_id: nil, connection_generation: nil,
          server_connection_id: nil, service_id: nil
        )
          new(
            payload[:command_name],
            payload[:database_name],
            address,
            payload[:request_id],
            operation_id,
            # All op_msg  payloads have a $db field. Legacy payloads do not
            # have a $db field. To emulate op_msg when publishing command
            # monitoring events for legacy servers, add $db to the payload,
            # copying the database name. Note that the database name is also
            # available as a top-level attribute on the command started event.
            payload[:command].merge('$db' => payload[:database_name]),
            socket_object_id: socket_object_id,
            connection_id: connection_id,
            connection_generation: connection_generation,
            server_connection_id: server_connection_id,
            service_id: service_id,
          )
        end

        # Returns a concise yet useful summary of the event.
        #
        # @return [ String ] String summary of the event.
        #
        # @since 2.6.0
        def inspect
          "#<{#{self.class} #{database_name}.#{command_name} command=#{command}>"
        end
      end
    end
  end
end

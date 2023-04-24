# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2015-2020 MongoDB Inc.
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
  module Operation

    # Shared executable behavior of operations.
    #
    # @since 2.5.2
    # @api private
    module Executable

      include ResponseHandling

      def do_execute(connection, context, options = {})
        session&.materialize_if_needed
        unpin_maybe(session, connection) do
          add_error_labels(connection, context) do
            add_server_diagnostics(connection) do
              get_result(connection, context, options).tap do |result|
                if session
                  if session.in_transaction? &&
                    connection.description.load_balancer?
                  then
                    if session.pinned_connection_global_id
                      unless session.pinned_connection_global_id == connection.global_id
                        raise(
                          Error::InternalDriverError,
                          "Expected operation to use connection #{session.pinned_connection_global_id} but it used #{connection.global_id}"
                        )
                      end
                    else
                      session.pin_to_connection(connection.global_id)
                      connection.pin
                    end
                  end

                  if session.snapshot? && !session.snapshot_timestamp
                    session.snapshot_timestamp = result.snapshot_timestamp
                  end
                end

                if result.has_cursor_id? &&
                  connection.description.load_balancer?
                then
                  if result.cursor_id == 0
                    connection.unpin
                  else
                    connection.pin
                  end
                end
                process_result(result, connection)
              end
            end
          end
        end
      end

      def execute(connection, context:, options: {})
        if Lint.enabled?
          unless connection.is_a?(Mongo::Server::Connection)
            raise Error::LintError, "Connection argument is of wrong type: #{connection}"
          end
        end

        do_execute(connection, context, options).tap do |result|
          validate_result(result, connection, context)
        end
      end

      private

      def result_class
        Result
      end

      def get_result(connection, context, options = {})
        result_class.new(*dispatch_message(connection, context, options))
      end

      # Returns a Protocol::Message or nil as reply.
      def dispatch_message(connection, context, options = {})
        message = build_message(connection, context)
        message = message.maybe_encrypt(connection, context)
        reply = connection.dispatch([ message ], context, options)
        [reply, connection.description, connection.global_id]
      end

      # @param [ Mongo::Server::Connection ] connection The connection on which
      #   the operation is performed.
      # @param [ Mongo::Operation::Context ] context The operation context.
      def build_message(connection, context)
        msg = message(connection)
        if server_api = context.server_api
          msg = msg.maybe_add_server_api(server_api)
        end
        msg
      end

      def process_result(result, connection)
        connection.server.update_cluster_time(result)

        process_result_for_sdam(result, connection)

        if session
          session.process(result)
        end

        result
      end

      def process_result_for_sdam(result, connection)
        if (result.not_master? || result.node_recovering?) &&
          connection.generation >= connection.server.pool.generation(service_id: connection.service_id)
        then
          if result.node_shutting_down?
            keep_pool = false
          else
            # Max wire version needs to be examined while the server is known
            keep_pool = connection.description.server_version_gte?('4.2')
          end

          connection.server.unknown!(
            keep_connection_pool: keep_pool,
            generation: connection.generation,
            service_id: connection.service_id,
            topology_version: result.topology_version,
          )

          connection.server.scan_semaphore.signal
        end
      end
    end
  end
end

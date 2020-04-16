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
    module Executable

      include ResponseHandling

      def do_execute(connection, client, options = {})
        unpin_maybe(session) do
          add_error_labels do
            add_server_diagnostics(connection.server) do
              get_result(connection, client, options).tap do |result|
                process_result(result, connection.server)
              end
            end
          end
        end
      end

      def execute(connection, client:, options: {})
        if Lint.enabled?
          unless connection.is_a?(Mongo::Server::Connection)
            raise Error::LintError, "Connection argument is of wrong type: #{connection}"
          end
        end

        do_execute(connection, client, options).tap do |result|
          validate_result(result, connection.server)
        end
      end

      private

      def result_class
        Result
      end

      def get_result(connection, client, options = {})
        result_class.new(*dispatch_message(connection, client, options))
      end

      # Returns a Protocol::Message or nil as reply.
      def dispatch_message(connection, client, options = {})
        message = build_message(connection)
        message = message.maybe_encrypt(connection, client)
        reply = connection.dispatch([ message ], operation_id, client, options)
        [reply, connection.description]
      end

      def build_message(connection)
        message(connection)
      end

      def process_result(result, server)
        server.update_cluster_time(result)

        if result.not_master? || result.node_recovering?
          if result.node_shutting_down?
            keep_pool = false
          else
            # Max wire version needs to be examined while the server is known
            keep_pool = server.description.server_version_gte?('4.2')
          end

          server.unknown!(keep_connection_pool: keep_pool)

          server.scan_semaphore.signal
        end

        session.process(result) if session
        result
      end
    end
  end
end

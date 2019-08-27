# Copyright (C) 2015-2019 MongoDB, Inc.
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

      def do_execute(server)
        unpin_maybe(session) do
          add_error_labels do
            get_result(server).tap do |result|
              process_result(result, server)
            end
          end
        end
      end

      def execute(server)
        do_execute(server).tap do |result|
          validate_result(result)
        end
      end

      private

      def result_class
        Result
      end

      def get_result(server)
        result_class.new(dispatch_message(server))
      end

      # Returns a Protocol::Message or nil
      def dispatch_message(server)
        server.with_connection do |connection|
          connection.dispatch([ build_message(server) ], operation_id)
        end
      end

      def build_message(server)
        message(server)
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

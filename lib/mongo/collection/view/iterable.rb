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
  class Collection
    class View

      # TODO: clean this up
      class InvalidServerError < Mongo::Error; end
      # Defines iteration related behavior for collection views, including
      # cursor instantiation.
      #
      # @since 2.0.0
      module Iterable
        # TODO: clean up
        private def valid_server?(server)
          server.standalone? || server.mongos? || server.primary? || secondary_ok?
        end

        # Iterate through documents returned by a query with this +View+.
        #
        # @example Iterate through the result of the view.
        #   view.each do |document|
        #     p document
        #   end
        #
        # @return [ Enumerator ] The enumerator.
        #
        # @since 2.0.0
        #
        # @yieldparam [ Hash ] Each matching document.
        def each
          @cursor = nil
          session = client.send(:get_session, @options)
          begin
            @cursor = if respond_to?(:write?, true) && write?
              server = server_selector.select_server(cluster, nil, session)

              result = server.with_connection do |connection|
                raise InvalidServerError.new unless valid_server?(connection)
                send_initial_query(connection, session)
              end

              Cursor.new(view, result, server, session: session)
            else
              read_with_retry_cursor(session, server_selector, view) do |connection|
                raise InvalidServerError.new unless valid_server?(connection)
                send_initial_query(connection, session)
              end
            end
          rescue InvalidServerError
            log_warn("Rerouting the Aggregation operation to the primary server - #{server.summary} is not suitable")
            server = cluster.next_primary(nil, session)

            result = server.with_connection do |connection|
              send_initial_query(connection, session)
            end

            if respond_to?(:write?, true) && write?
              @cursor = Cursor.new(view, result, server, session: session)
            end
          end

          if block_given?
            @cursor.each do |doc|
              yield doc
            end
          else
            @cursor.to_enum
          end
        end

        # Cleans up resources associated with this query.
        #
        # If there is a server cursor associated with this query, it is
        # closed by sending a KillCursors command to the server.
        #
        # @note This method propagates any errors that occur when closing the
        #   server-side cursor.
        #
        # @return [ nil ] Always nil.
        #
        # @raise [ Error::OperationFailure ] If the server cursor close fails.
        #
        # @since 2.1.0
        def close_query
          if @cursor
            @cursor.close
          end
        end
        alias :kill_cursors :close_query

        private

        def initial_query_op(server, session)
          if server.features.find_command_enabled?
            initial_command_op(session)
          else
            Operation::Find.new(Builder::OpQuery.new(self).specification)
          end
        end

        def initial_command_op(session)
          if explained?
            Operation::Explain.new(Builder::FindCommand.new(self, session).explain_specification)
          else
            Operation::Find.new(Builder::FindCommand.new(self, session).specification)
          end
        end

        def send_initial_query(server, session = nil)
          validate_collation!(server, collation)
          initial_query_op(server, session).execute(server, client: client)
        end
      end
    end
  end
end

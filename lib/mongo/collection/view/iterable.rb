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

      # Defines iteration related behavior for collection views, including
      # cursor instantiation.
      #
      # @since 2.0.0
      module Iterable

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
          if QueryCache.enabled?
            unless @cursor = cached_cursor
              @cursor = select_cursor(session)
              QueryCache.cache_table[cache_key] = @cursor
            end
            range = limit || nil
          else
            @cursor = select_cursor(session)
          end
          if block_given?
            if !range
              @cursor.each do |doc|
                yield doc
              end
            else
              @cursor.to_a[0...range].each do |doc|
                yield doc
              end
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

        def select_cursor(session)
          if respond_to?(:write?, true) && write?
            server = server_selector.select_server(cluster, nil, session)
            result = send_initial_query(server, session)
            if QueryCache.enabled?
              CachingCursor.new(view, result, server, session: session)
            else
              Cursor.new(view, result, server, session: session)
            end
          else
            read_with_retry_cursor(session, server_selector, view) do |server|
              send_initial_query(server, session)
            end
          end
        end

        def cached_cursor
          if limit
            key = [ collection.namespace, selector, nil, skip, sort, projection, collation  ]
            cursor = QueryCache.cache_table[key]
          end
          cursor || QueryCache.cache_table[cache_key]
        end

        def cache_key
          [ collection.namespace, selector, limit, skip, sort, projection, collation ]
        end

        def initial_query_op(server, session)
          if server.with_connection { |connection| connection.features }.find_command_enabled?
            initial_command_op(session)
          else
            # Server versions that do not have the find command feature
            # (versions older than 3.2) do not support the allow_disk_use option
            # but perform no validation and will not raise an error if it is
            # specified. If the allow_disk_use option is specified, raise an error
            # to alert the user.
            raise Error::UnsupportedOption.allow_disk_use_error if options.key?(:allow_disk_use)
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

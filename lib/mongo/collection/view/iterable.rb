# frozen_string_literal: true
# encoding: utf-8

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
          # If the caching cursor is closed and was not fully iterated,
          # the documents we have in it are not the complete result set and
          # we have no way of completing that iteration.
          # Therefore, discard that cursor and start iteration again.
          # The case of the caching cursor not being closed and not having
          # been fully iterated isn't tested - see RUBY-2773.
          @cursor = if use_query_cache? && cached_cursor && (
            cached_cursor.fully_iterated? || !cached_cursor.closed?
          )
            cached_cursor
          else
            session = client.send(:get_session, @options)
            select_cursor(session).tap do |cursor|
              if use_query_cache?
                # No need to store the cursor in the query cache if there is
                # already a cached cursor stored at this key.
                QueryCache.set(cursor, **cache_options)
              end
            end
          end

          if use_query_cache?
            # If a query with a limit is performed, the query cache will
            # re-use results from an earlier query with the same or larger
            # limit, and then impose the lower limit during iteration.
            limit_for_cached_query = respond_to?(:limit) ? limit : nil
          end

          if block_given?
            # Ruby versions 2.5 and older do not support arr[0..nil] syntax, so
            # this must be a separate conditional.
            cursor_to_iterate = if limit_for_cached_query
              @cursor.to_a[0...limit_for_cached_query]
            else
              @cursor
            end

            cursor_to_iterate.each do |doc|
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

        def select_cursor(session)
          if respond_to?(:write?, true) && write?
            server = server_selector.select_server(cluster, nil, session)
            result = send_initial_query(server, session)

            # RUBY-2367: This will be updated to allow the query cache to
            # cache cursors with multi-batch results.
            if use_query_cache?
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
          QueryCache.get(**cache_options)
        end

        def cache_options
          # NB: this hash is passed as keyword argument and must have symbol
          # keys.
          {
            namespace: collection.namespace,
            selector: selector,
            skip: skip,
            sort: sort,
            limit: limit,
            projection: projection,
            collation: collation,
            read_concern: read_concern,
            read_preference: read_preference

          }
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
          builder = Builder::FindCommand.new(self, session)
          if explained?
            Operation::Explain.new(builder.explain_specification)
          else
            Operation::Find.new(builder.specification)
          end
        end

        def send_initial_query(server, session = nil)
          validate_collation!(server, collation)
          initial_query_op(server, session).execute(server, context: Operation::Context.new(client: client, session: session))
        end

        def use_query_cache?
          QueryCache.enabled? && !collection.system_collection?
        end
      end
    end
  end
end

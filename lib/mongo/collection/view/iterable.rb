# frozen_string_literal: true
# rubocop:todo all

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

require 'mongo/cursor_host'

module Mongo
  class Collection
    class View

      # Defines iteration related behavior for collection views, including
      # cursor instantiation.
      #
      # @since 2.0.0
      module Iterable
        include Mongo::CursorHost

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
          @cursor = prefer_cached_cursor? ? cached_cursor : new_cursor_for_iteration
          return @cursor.to_enum unless block_given?

          limit_for_cached_query = compute_limit_for_cached_query

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
        # @raise [ Error::OperationFailure::Family ] If the server cursor close fails.
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
          context = Operation::Context.new(
            client: client,
            session: session,
            operation_timeouts: operation_timeouts,
            view: self
          )

          if respond_to?(:write?, true) && write?
            server = server_selector.select_server(cluster, nil, session, write_aggregation: true)
            result = send_initial_query(server, context)

            if use_query_cache?
              CachingCursor.new(view, result, server, session: session, context: context)
            else
              Cursor.new(view, result, server, session: session, context: context)
            end
          else
            read_with_retry_cursor(session, server_selector, view, context: context) do |server|
              send_initial_query(server, context)
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
            read_preference: read_preference,
          }
        end

        def initial_query_op(session)
          spec = {
            coll_name: collection.name,
            filter: filter,
            projection: projection,
            db_name: database.name,
            session: session,
            collation: collation,
            sort: sort,
            skip: skip,
            let: options[:let],
            limit: limit,
            allow_disk_use: options[:allow_disk_use],
            allow_partial_results: options[:allow_partial_results],
            read: read,
            read_concern: options[:read_concern] || read_concern,
            batch_size: batch_size,
            hint: options[:hint],
            max_scan: options[:max_scan],
            max_value: options[:max_value],
            min_value: options[:min_value],
            no_cursor_timeout: options[:no_cursor_timeout],
            return_key: options[:return_key],
            show_disk_loc: options[:show_disk_loc],
            comment: options[:comment],
            oplog_replay: oplog_replay
          }

          if spec[:oplog_replay]
            collection.client.log_warn("The :oplog_replay option is deprecated and ignored by MongoDB 4.4 and later")
          end

          maybe_set_tailable_options(spec)

          if explained?
            spec[:explain] = options[:explain]
            Operation::Explain.new(spec)
          else
            Operation::Find.new(spec)
          end
        end

        def send_initial_query(server, context)
          operation = initial_query_op(context.session)
          if server.load_balancer?
            # Connection will be checked in when cursor is drained.
            connection = server.pool.check_out(context: context)
            operation.execute_with_connection(connection, context: context)
          else
            operation.execute(server, context: context)
          end
        end

        def use_query_cache?
          QueryCache.enabled? && !collection.system_collection?
        end

        # If the caching cursor is closed and was not fully iterated,
        # the documents we have in it are not the complete result set and
        # we have no way of completing that iteration.
        # Therefore, discard that cursor and start iteration again.
        def prefer_cached_cursor?
          use_query_cache? &&
            cached_cursor &&
            (cached_cursor.fully_iterated? || !cached_cursor.closed?)
        end

        # Start a new cursor for use when iterating (via #each).
        def new_cursor_for_iteration
          session = client.get_session(@options)
          select_cursor(session).tap do |cursor|
            if use_query_cache?
              # No need to store the cursor in the query cache if there is
              # already a cached cursor stored at this key.
              QueryCache.set(cursor, **cache_options)
            end
          end
        end

        def compute_limit_for_cached_query
          return nil unless use_query_cache? && respond_to?(:limit)

          # If a query with a limit is performed, the query cache will
          # re-use results from an earlier query with the same or larger
          # limit, and then impose the lower limit during iteration.
          return QueryCache.normalized_limit(limit)
        end

        # Add tailable cusror options to the command specifiction if needed.
        #
        # @param [ Hash ] spec The command specification.
        def maybe_set_tailable_options(spec)
          case cursor_type
          when :tailable
            spec[:tailable] = true
          when :tailable_await
            spec[:tailable] = true
            spec[:await_data] = true
          end
        end

        # @return [ true | false | nil ] options[:oplog_replay], if
        #    set, otherwise the same option from the collection.
        def oplog_replay
          v = options[:oplog_replay]
          v.nil? ? collection.options[:oplog_replay] : v
        end
      end
    end
  end
end

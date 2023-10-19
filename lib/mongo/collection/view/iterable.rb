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

module Mongo
  class Collection
    class View

      # Defines iteration related behavior for collection views, including
      # cursor instantiation.
      #
      # @since 2.0.0
      module Iterable

        # Returns the cursor associated with this view, if any.
        #
        # @return [ nil | Cursor ] The cursor, if any.
        #
        # @api private
        attr_reader :cursor

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
            limit_for_cached_query = respond_to?(:limit) ? QueryCache.normalized_limit(limit) : nil
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
            server = server_selector.select_server(cluster, nil, session, write_aggregation: true)
            result = send_initial_query(server, session)

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
            read: read,
            read_concern: options[:read_concern] || read_concern,
            batch_size: batch_size,
            hint: options[:hint],
            max_scan: options[:max_scan],
            max_time_ms: options[:max_time_ms],
            max_value: options[:max_value],
            min_value: options[:min_value],
            no_cursor_timeout: options[:no_cursor_timeout],
            return_key: options[:return_key],
            show_disk_loc: options[:show_disk_loc],
            comment: options[:comment],
            oplog_replay: if (v = options[:oplog_replay]).nil?
              collection.options[:oplog_replay]
            else
              v
            end,
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

        def send_initial_query(server, session = nil)
          initial_query_op(session).execute(server, context: Operation::Context.new(client: client, session: session))
        end

        def use_query_cache?
          QueryCache.enabled? && !collection.system_collection?
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
      end
    end
  end
end

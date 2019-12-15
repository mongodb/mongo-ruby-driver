# Copyright (C) 2019 MongoDB, Inc.
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

  module QueryCache
    class << self

      def activate!
        Collection.module_eval do
          extend QueryCache::Base
          alias_query_cache_clear :insert_one, :insert_many
        end

        Collection::View.module_eval do
          extend QueryCache::Base
          alias_query_cache_clear :delete_one, :delete_many,
            :update_one, :update_many,
            :replace_one,
            :find_one_and_delete, :find_one_and_replace, :find_one_and_update
        end
      end

      # Execute the block while using the query cache.
      #
      # @example Execute with the cache.
      #   QueryCache.cache { collection.find }
      #
      # @return [ Object ] The result of the block.
      #
      # @since 4.0.0
      def cache
        enabled = QueryCache.enabled?
        QueryCache.enabled = true
        yield
      ensure
        QueryCache.enabled = enabled
      end

      # Execute the block with the query cache disabled.
      #
      # @example Execute without the cache.
      #   QueryCache.uncached { collection.find }
      #
      # @return [ Object ] The result of the block.
      def uncached
        enabled = QueryCache.enabled?
        QueryCache.enabled = false
        yield
      ensure
        QueryCache.enabled = enabled
      end

      # Clear the query cache.
      #
      # @example Clear the cache.
      #   QueryCache.clear_cache
      #
      # @return [ nil ] Always nil.
      def clear_cache
        Thread.current["[mongo]:query_cache"] = nil
      end

      # Set whether the cache is enabled.
      #
      # @example Set if the cache is enabled.
      #   QueryCache.enabled = true
      #
      # @param [ true, false ] value The enabled value.
      #
      # @since 4.0.0
      def enabled=(value)
        Thread.current["[mongo]:query_cache:enabled"] = value
      end

      # Is the query cache enabled on the current thread?
      #
      # @example Is the query cache enabled?
      #   QueryCache.enabled?
      #
      # @return [ true, false ] If the cache is enabled.
      #
      # @since 4.0.0
      def enabled?
        !!Thread.current["[mongo]:query_cache:enabled"]
      end

      # Get the cached queries.
      #
      # @example Get the cached queries from the current thread.
      #   QueryCache.cache_table
      #
      # @return [ Hash ] The hash of cached queries.
      #
      # @api private
      def cache_store
        Thread.current["[mongo]:query_cache"] ||= {}
      end
    end

    # Included to add behavior for clearing out the query cache on certain
    # operations.
    #
    # @since 4.0.0
    module Base

      def alias_query_cache_clear(*method_names)
        method_names.each do |method_name|
          define_method("#{method_name}_with_clear_cache") do |*args|
            QueryCache.clear_cache
            send("#{method_name}_without_clear_cache", *args)
          end

          alias_method "#{method_name}_without_clear_cache", method_name
          alias_method method_name, "#{method_name}_with_clear_cache"
        end
      end
    end

    # Contains enhancements to the Mongo::Collection::View in order to get a
    # cached cursor or a regular cursor on iteration.
    #
    # @since 5.0.0
    module View

      # Override the default enumeration to handle if the cursor can be cached
      # or not.
      #
      # @example Iterate over the view.
      #   view.each do |doc|
      #     # ...
      #   end
      #
      # @since 5.0.0
      def each
        if system_collection? || !QueryCache.enabled?
          super
        else
          unless cursor = cached_cursor
            read_with_retry do
              server = server_selector.select_server(cluster)
              cursor = Mongo::CachingCursor.new(view, send_initial_query(server), server)
              QueryCache.cache_table[cache_key] = cursor
            end
          end
          cursor.each do |doc|
            yield doc
          end if block_given?
          cursor
        end
      end

      private

      def cached_cursor
        if limit
          key = [ collection.namespace, selector, nil, skip, sort, projection, collation  ]
          cursor = QueryCache.cache_table[key]
          if cursor
            cursor.to_a[0...limit.abs]
          end
        end
        cursor || QueryCache.cache_table[cache_key]
      end

      def cache_key
        [ collection.namespace, selector, limit, skip, sort, projection, collation ]
      end

      def system_collection?
        collection.namespace =~ /\Asystem./
      end
    end
  end
end

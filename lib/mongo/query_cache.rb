# Copyright (C) 2020 MongoDB, Inc.
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

      # Set whether the cache is enabled.
      #
      # @example Set if the cache is enabled.
      #   QueryCache.enabled = true
      #
      # @param [ true, false ] value The enabled value.
      def enabled=(value)
        Thread.current["[mongo]:query_cache:enabled"] = value
      end

      # Is the query cache enabled on the current thread?
      #
      # @example Is the query cache enabled?
      #   QueryCache.enabled?
      #
      # @return [ true, false ] If the cache is enabled.
      def enabled?
        !!Thread.current["[mongo]:query_cache:enabled"]
      end

      # Execute the block while using the query cache.
      #
      # @example Execute with the cache.
      #   QueryCache.cache { collection.find }
      #
      # @return [ Object ] The result of the block.
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

      # Get the cached queries.
      #
      # @example Get the cached queries from the current thread.
      #   QueryCache.cache_table
      #
      # @return [ Hash ] The hash of cached queries.
      def cache_table
        Thread.current["[mongo]:query_cache"] ||= {}
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

      # Clear the section of the query cache storing cursors with results
      # from this namespace.
      #
      # @param [ String ] namespace The namespace to be cleared, in the format
      #   "database.collection".
      #
      # @return [ nil ] Always nil.
      #
      # @api private
      def clear_namespace(namespace)
        cache_table[namespace] = nil if cache_table[namespace]
      end

      # Store a CachingCursor instance in the query cache.
      #
      # @param [ Mongo::CachingCursor ] cursor The CachingCursor instance to store.
      # @param [ Hash ] options The query options that will be used to create
      #   the cache key. Valid keys are: :namespace, :selector, :skip, :sort,
      #   :limit, :projection, :collation, :read_concern, and :read_preference.
      #
      # @return [ true ] Always true.
      #
      # @api private
      def set(cursor, options = {})
        key = cache_key(options)
        namespace = options[:namespace]

        QueryCache.cache_table[namespace] ||= {}
        QueryCache.cache_table[namespace][key] = cursor

        true
      end

      # For the given query options, determine whether the cache has stored a
      # CachingCursor that can be used to acquire the correct query results.
      #
      # @param [ Hash ] options The query options that will be used to create
      #   the cache key. Valid keys are: :namespace, :selector, :skip, :sort,
      #   :limit, :projection, :collation, :read_concern, and :read_preference.
      #
      # @return [ Mongo::CachingCursor | nil ] Returns a CachingCursor if one
      #   exists in the query cache, otherwise returns nil.
      #
      # @api private
      def get(options = {})
        limit = options[:limit]
        namespace = options[:namespace]
        key = cache_key(options)

        namespace_hash = QueryCache.cache_table[namespace]
        return nil unless namespace_hash

        caching_cursor = namespace_hash[key]
        return nil unless caching_cursor

        caching_cursor_limit = caching_cursor.view.limit

        # There are two scenarios in which a caching cursor could fulfill the
        # query:
        # 1. The query has a limit, and the stored cursor has no limit or
        #    a larger limit.
        # 2. The query has no limit and the stored cursor has no limit.
        #
        # Otherwise, return nil because the stored cursor will not satisfy
        # the query.
        if limit && (caching_cursor_limit.nil? || caching_cursor_limit >= limit)
          caching_cursor
        elsif limit.nil? && caching_cursor_limit.nil?
          caching_cursor
        else
          nil
        end
      end

      private

      def cache_key(options)
        unless options[:namespace] && options[:selector]
          raise ArgumentError.new("Cannot generate cache key without namespace or selector")
        end

        [
          options[:selector],
          options[:skip],
          options[:sort],
          options[:projection],
          options[:collation],
          options[:read_concern],
          options[:read_preference]
        ]
      end
    end
  end
end

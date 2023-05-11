# frozen_string_literal: true
# rubocop:todo all

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
        enabled = enabled?
        self.enabled = true
        begin
          yield
        ensure
          self.enabled = enabled
        end
      end

      # Execute the block with the query cache disabled.
      #
      # @example Execute without the cache.
      #   QueryCache.uncached { collection.find }
      #
      # @return [ Object ] The result of the block.
      def uncached
        enabled = enabled?
        self.enabled = false
        begin
          yield
        ensure
          self.enabled = enabled
        end
      end

      # Get the cached queries.
      #
      # @example Get the cached queries from the current thread.
      #   QueryCache.cache_table
      #
      # @return [ Hash ] The hash of cached queries.
      private def cache_table
        Thread.current["[mongo]:query_cache"] ||= {}
      end

      # Clear the query cache.
      #
      # @example Clear the cache.
      #   QueryCache.clear
      #
      # @return [ nil ] Always nil.
      def clear
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
        cache_table.delete(namespace)
        # The nil key is where cursors are stored that could potentially read from
        # multiple collections. This key should be cleared on every write operation
        # to prevent returning stale data.
        cache_table.delete(nil)
        nil
      end

      # Store a CachingCursor instance in the query cache associated with the
      # specified query options.
      #
      # @param [ Mongo::CachingCursor ] cursor The CachingCursor instance to store.
      #
      # @option opts [ String | nil ] :namespace The namespace of the query,
      #   in the format "database_name.collection_name".
      # @option opts [ Array, Hash ] :selector The selector passed to the query.
      #   For most queries, this will be a Hash, but for aggregations, this
      #   will be an Array representing the aggregation pipeline. May not be nil.
      # @option opts [ Integer | nil ] :skip The skip value of the query.
      # @option opts [ Hash | nil ] :sort The order of the query results
      #   (e.g. { name: -1 }).
      # @option opts [ Integer | nil ] :limit The limit value of the query.
      # @option opts [ Hash | nil ] :projection The projection of the query
      #   results (e.g. { name: 1 }).
      # @option opts [ Hash | nil ] :collation The collation of the query
      #   (e.g. { "locale" => "fr_CA" }).
      # @option opts [ Hash | nil ] :read_concern The read concern of the query
      #   (e.g. { level: :majority }).
      # @option opts [ Hash | nil ] :read_preference The read preference of
      #   the query (e.g. { mode: :secondary }).
      # @option opts [ Boolean | nil ] :multi_collection Whether the query
      #   results could potentially come from multiple collections. When true,
      #   these results will be stored under the nil namespace key and cleared
      #   on every write command.
      #
      # @return [ true ] Always true.
      #
      # @api private
      def set(cursor, **opts)
        _cache_key = cache_key(**opts)
        _namespace_key = namespace_key(**opts)

        cache_table[_namespace_key] ||= {}
        cache_table[_namespace_key][_cache_key] = cursor

        true
      end

      # For the given query options, retrieve a cached cursor that can be used
      # to obtain the correct query results, if one exists in the cache.
      #
      # @option opts [ String | nil ] :namespace The namespace of the query,
      #   in the format "database_name.collection_name".
      # @option opts [ Array, Hash ] :selector The selector passed to the query.
      #   For most queries, this will be a Hash, but for aggregations, this
      #   will be an Array representing the aggregation pipeline. May not be nil.
      # @option opts [ Integer | nil ] :skip The skip value of the query.
      # @option opts [ Hash | nil ] :sort The order of the query results
      #   (e.g. { name: -1 }).
      # @option opts [ Integer | nil ] :limit The limit value of the query.
      # @option opts [ Hash | nil ] :projection The projection of the query
      #   results (e.g. { name: 1 }).
      # @option opts [ Hash | nil ] :collation The collation of the query
      #   (e.g. { "locale" => "fr_CA" }).
      # @option opts [ Hash | nil ] :read_concern The read concern of the query
      #   (e.g. { level: :majority }).
      # @option opts [ Hash | nil ] :read_preference The read preference of
      #   the query (e.g. { mode: :secondary }).
      # @option opts [ Boolean | nil ] :multi_collection Whether the query
      #   results could potentially come from multiple collections. When true,
      #   these results will be stored under the nil namespace key and cleared
      #   on every write command.
      #
      # @return [ Mongo::CachingCursor | nil ] Returns a CachingCursor if one
      #   exists in the query cache, otherwise returns nil.
      #
      # @api private
      def get(**opts)
        limit = normalized_limit(opts[:limit])

        _namespace_key = namespace_key(**opts)
        _cache_key = cache_key(**opts)

        namespace_hash = cache_table[_namespace_key]
        return nil unless namespace_hash

        caching_cursor = namespace_hash[_cache_key]
        return nil unless caching_cursor

        caching_cursor_limit = normalized_limit(caching_cursor.view.limit)

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

      def normalized_limit(limit)
        return nil unless limit
        # For the purposes of caching, a limit of 0 means no limit, as mongo treats it as such.
        return nil if limit == 0
        # For the purposes of caching, a negative limit is the same as as a positive limit.
        limit.abs
      end

      private

      def cache_key(**opts)
        unless opts[:namespace]
          raise ArgumentError.new("Cannot generate cache key without namespace")
        end
        unless opts[:selector]
          raise ArgumentError.new("Cannot generate cache key without selector")
        end

        [
          opts[:namespace],
          opts[:selector],
          opts[:skip],
          opts[:sort],
          opts[:projection],
          opts[:collation],
          opts[:read_concern],
          opts[:read_preference]
        ]
      end

      # If the cached results can come from multiple collections, store this
      # cursor under the nil namespace to be cleared on every write operation.
      # Otherwise, store it under the specified namespace.
      def namespace_key(**opts)
        if opts[:multi_collection]
          nil
        else
          opts[:namespace]
        end
      end
    end

    # Rack middleware that activates the query cache for each request.
    class Middleware

      # Instantiate the middleware.
      #
      # @example Create the new middleware.
      #   Middleware.new(app)
      #
      # @param [ Object ] app The rack application stack.
      def initialize(app)
        @app = app
      end

      # Enable query cache and execute the request.
      #
      # @example Execute the request.
      #   middleware.call(env)
      #
      # @param [ Object ] env The environment.
      #
      # @return [ Object ] The result of the call.
      def call(env)
        QueryCache.cache do
          @app.call(env)
        end
      ensure
        QueryCache.clear
      end

      # ActiveJob middleware that activates the query cache for each job.
      module ActiveJob
        def self.included(base)
          base.class_eval do
            around_perform do |_job, block|
              QueryCache.cache do
                block.call
              end
            ensure
              QueryCache.clear
            end
          end
        end
      end
    end
  end
end

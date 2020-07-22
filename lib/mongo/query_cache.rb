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
    end
  end
end

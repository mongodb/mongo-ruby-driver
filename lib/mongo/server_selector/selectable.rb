# Copyright (C) 2014-2015 MongoDB, Inc.
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

  module ServerSelector

    # Provides common behavior for filtering a list of servers by server mode or tag set.
    #
    # @since 2.0.0
    module Selectable

      # The max latency in milliseconds between the closest server and other servers
      # considered for selection.
      #
      # @since 2.0.0
      LOCAL_THRESHOLD_MS = 15

      # How long to block for server selection before throwing an exception.
      #
      # @since 2.0.0
      SERVER_SELECTION_TIMEOUT_MS = 30000

      # @return [ Array ] tag_sets The tag sets used to select servers.
      attr_reader :tag_sets

      # Check equality of two server selector.
      #
      # @example Check server selector equality.
      #   preference == other
      #
      # @param [ Object ] other The other preference.
      #
      # @return [ true, false ] Whether the objects are equal.
      #
      # @since 2.0.0
      def ==(other)
        name == other.name &&
            tag_sets == other.tag_sets
      end

      # Initialize the server selector.
      #
      # @example Initialize the preference with tag sets.
      #   Mongo::ServerSelector::Secondary.new([{ 'tag' => 'set' }])
      #
      # @example Initialize the preference with no options.
      #   Mongo::ServerSelector::Secondary.new
      #
      # @param [ Array ] tag_sets The tag sets used to select servers.
      #
      # @todo: document specific error
      # @raise [ Exception ] If tag sets are specified but not allowed.
      #
      # @since 2.0.0
      def initialize(tag_sets = [], options = {})
        raise ServerSelector::InvalidServerPreference.new(name) if !tag_sets.empty? && !tags_allowed?
        @tag_sets = tag_sets
      end

      # Select a server from eligible candidates.
      #
      # @param [ Mongo::Cluster ] cluster The cluster from which to select an eligible server.
      #
      # @return [ Mongo::Server ] A server matching the server preference.
      #
      # @since 2.0.0
      def select_server(cluster)
        @server_selection_timeout_ms = cluster.options[:server_selection_timeout_ms] ||
                                         SERVER_SELECTION_TIMEOUT_MS
        @local_threshold_ms = cluster.options[:local_threshold_ms] ||
                                LOCAL_THRESHOLD_MS

        now = Time.now
        end_time = now + @server_selection_timeout_ms # TODO: wrong arithmetic

        while now < end_time
          if cluster.standalone?
            servers = cluster.servers
          elsif cluster.sharded?
            servers = near_servers(cluster.servers).shuffle!
          else
            select(cluster.servers).shuffle!
          end

          return servers.first unless servers.nil? || servers.empty?
          # cluster.rescan...?
          now = Time.now
        end

        raise NoServerAvailable.new(self) if servers.nil? || servers.empty?
      end

      private

      # Select the primary from a list of provided candidates.
      #
      # @param [ Array ] candidates List of candidate servers to select the
      #   primary from.
      #
      # @return [ Array ] The primary.
      #
      # @since 2.0.0
      def primary(candidates)
        candidates.select do |server|
          server.primary? || server.standalone?
        end
      end

      # Select the secondaries from a list of provided candidates.
      #
      # @param [ Array ] candidates List of candidate servers to select the
      #   secondaries from.
      #
      # @return [ Array ] The secondary servers.
      #
      # @since 2.0.0
      def secondaries(candidates)
        matching_servers = candidates.select(&:secondary?)
        matching_servers = match_tag_sets(matching_servers) unless tag_sets.empty?
        matching_servers
      end

      # Select the near servers from a list of provided candidates, taking the
      #   local threshold into account.
      #
      # @param [ Array ] candidates List of candidate servers to select the
      #   near servers from.
      #
      # @return [ Array ] The near servers.
      #
      # @since 2.0.0
      def near_servers(candidates = [])
        return candidates if candidates.empty?
        nearest_server = candidates.min_by(&:round_trip_time)
        threshold = nearest_server.round_trip_time + (@local_threshold_ms || LOCAL_THRESHOLD_MS)
        candidates.select { |server| server.round_trip_time <= threshold }
      end

      # Select the servers matching the defined tag sets.
      #
      # @param [ Array ] candidates List of candidate servers from which those
      #   matching the defined tag sets should be selected.
      #
      # @return [ Array ] The servers matching the defined tag sets.
      #
      # @since 2.0.0
      def match_tag_sets(candidates)
        matches = []
        tag_sets.find do |tag_set|
          matches = candidates.select { |server| server.matches_tags?(tag_set) }
          !matches.empty?
        end
        matches || []
      end
    end

    class InvalidServerPreference < MongoError

      # Instantiate the new exception.
      #
      # @example Instantiate the exception.
      #   Mongo::ServerSelector::InvalidServerPreference.new
      #
      # @since 2.0.0
      def initialize(name)
        super("This server preference #{mode} cannot be combined with tags.")
      end
    end
  end
end

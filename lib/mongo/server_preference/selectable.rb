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

  module ServerPreference

    # Provides common behavior for filtering a list of servers by server mode or tag set.
    #
    # @since 2.0.0
    module Selectable

      # @return [ Array ] tag_sets The tag sets used to select servers.
      attr_reader :tag_sets

      # @return [ Integer ] local_threshold_ms The max latency in milliseconds between
      #   the closest secondary and other secondaries considered for selection.
      attr_reader :local_threshold_ms

      # @return [ Integer ] server_selection_timeout_ms How long to block for server selection
      #   before throwing an exception. The default is 30,000 (milliseconds).
      attr_reader :server_selection_timeout_ms

      # Check equality of two server preferences.
      #
      # @example Check server preference equality.
      #   preference == other
      #
      # @param [ Object ] other The other preference.
      #
      # @return [ true, false ] Whether the objects are equal.
      #
      # @since 2.0.0
      def ==(other)
        name == other.name &&
            tag_sets == other.tag_sets &&
            local_threshold_ms == other.local_threshold_ms &&
            server_selection_timeout_ms == other.server_selection_timeout_ms
      end

      # Initialize the server preference.
      #
      # @example Initialize the preference with tag sets.
      #   Mongo::ServerPreference::Secondary.new([{ 'tag' => 'set' }])
      #
      # @example Initialize the preference with local threshold
      #   Mongo::ServerPreference::Secondary.new([], 20)
      #
      # @example Initialize the preference with no options.
      #   Mongo::ServerPreference::Secondary.new
      #
      # @param [ Array ] tag_sets The tag sets used to select servers.
      # @param [ Integer ] local_threshold_ms (15) The max latency in milliseconds
      #   between the closest secondary and other secondaries considered for selection.
      # @param [ Integer ] server_selection_timeout_ms (30000) How long to block for
      #   server selection before throwing an exception
      #
      # @todo: document specific error
      # @raise [ Exception ] If tag sets are specified but not allowed.
      #
      # @since 2.0.0
      def initialize(tag_sets = [], local_threshold_ms = 15, server_selection_timeout_ms = 30000)
        # @todo: raise specific Exception
        raise Exception, "server preference #{name} cannot be combined " +
            " with tags" if !tag_sets.empty? && !tags_allowed?
        @tag_sets = tag_sets
        @local_threshold_ms = local_threshold_ms
        @server_selection_timeout_ms = server_selection_timeout_ms
      end

      # Select the primary from a list of provided candidates.
      #
      # @param [ Array ] candidates List of candidate servers to select the
      #   primary from.
      #
      # @return [ Array ] The primary.
      #
      # @since 2.0.0
      def primary(candidates)
        candidates.select{ |server| server.primary? || server.standalone? }
      end

      # Select ta server from eligible candidates.
      #
      # @param [ Mongo::Cluster ] cluster The cluster from which to select an eligible server.
      #
      # @return [ Mongo::Server ] A server matching the read preference.
      #
      # @since 2.0.0
      def select_server(cluster)
        return cluster.servers.first if cluster.standalone?
        return near_servers(cluster.servers) if cluster.sharded?
        servers = select(cluster.servers)
        raise NoServerAvailable.new(self) if servers.empty?
        servers.shuffle!.first
      end

      private

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
        threshold = nearest_server.round_trip_time + local_threshold_ms
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
  end
end

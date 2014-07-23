# Copyright (C) 2009-2014 MongoDB, Inc.
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
      # @return [ Integer ] acceptable_latency The max latency in milliseconds between
      #   the closest secondary and other secondaries considered for selection.
      attr_reader :acceptable_latency

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
            acceptable_latency == other.acceptable_latency
      end

      # Initialize the server preference.
      #
      # @example Initialize the preference with tag sets.
      #   Mongo::ServerPreference::Secondary.new([{ 'tag' => 'set' }])
      #
      # @example Initialize the preference with acceptable latency
      #   Mongo::ServerPreference::Secondary.new([], 20)
      #
      # @example Initialize the preference with no options.
      #   Mongo::ServerPreference::Secondary.new
      #
      # @param [ Array ] tag_sets The tag sets used to select servers.
      # @param [ Integer ] acceptable_latency (15) The max latency in milliseconds
      #   between the closest secondary and other secondaries considered for selection.
      #
      # @todo: document specific error
      # @raise [ Exception ] If tag sets are specified but not allowed.
      #
      # @since 2.0.0
      def initialize(tag_sets = [], acceptable_latency = 15)
        # @todo: raise specific Exception
        raise Exception, "server preference #{name} cannot be combined " +
            " with tags" if !tag_sets.empty? && !tags_allowed?
        @tag_sets = tag_sets
        @acceptable_latency = acceptable_latency
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
        candidates.select{ |server| server.primary? || server.standalone? }
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
      #   acceptable latency into account.
      #
      # @param [ Array ] candidates List of candidate servers to select the
      #   near servers from.
      #
      # @return [ Array ] The near servers.
      #
      # @since 2.0.0
      def near_servers(candidates = [])
        return candidates if candidates.empty?
        nearest_server = candidates.min_by(&:ping_time)
        max_latency = nearest_server.ping_time + acceptable_latency
        near_servers = candidates.select { |server| server.ping_time <= max_latency }
        near_servers.shuffle!
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

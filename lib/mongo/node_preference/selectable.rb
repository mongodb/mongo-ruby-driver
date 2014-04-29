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

  module NodePreference

    # Provides common behavior for filtering a list of nodes by server mode or tag set.
    #
    # @since 3.0.0
    module Selectable

      # @return [ Array ] tag_sets The tag sets used to select nodes.
      attr_reader :tag_sets
      # @return [ Integer ] acceptable_latency The max latency in milliseconds between
      #   the closest secondary and other secondaries considered for selection.
      attr_reader :acceptable_latency

      # Check equality of two node preferences.
      #
      # @example Check node preference equality
      #   pref == other
      #
      # @param [ Object ] other The other object.
      #
      # @return [ true, false ] Whether the objects are equal.
      #
      # @since 3.0.0
      def ==(other)
        name == other.name &&
            tag_sets == other.tag_sets &&
            acceptable_latency == other.acceptable_latency
      end

      # Initialize the node preference.
      #
      # @example Initialize the preference with tag sets.
      #   Mongo::NodePreference::Secondary.new([{'tag' => 'set'}] todo)
      #
      # @example Initialize the preference with acceptable latency
      #   Mongo::NodePreference::Secondary.new([], 20)
      #
      # @example Initialize the preference with no options.
      #   Mongo::NodePreference::Secondary
      #
      # @param [ Array ] tag_sets The tag sets used to select nodes.
      # @param [ Integer ] acceptable_latency The max latency in milliseconds between
      #   the closest secondary and other secondaries considered for selection.
      #
      # @since 3.0.0
      def initialize(tag_sets = [], acceptable_latency = 15)
        raise Exception, "Node preference #{name} cannot be combined " +
            " with tags" if !tag_sets.empty? && !tags_allowed?
        @tag_sets = tag_sets
        @acceptable_latency = acceptable_latency
      end

      private

      def primary(candidates)
        candidates.select(&:primary?)
      end

      def secondaries(candidates)
        matching_nodes = candidates.select(&:secondary?)
        matching_nodes = match_tag_sets(matching_nodes) unless tag_sets.empty?
        matching_nodes
      end

      def near_nodes(candidates = [])
        return candidates if candidates.empty?
        nearest_node = candidates.min_by(&:ping_time)
        max_latency = nearest_node.ping_time + acceptable_latency
        near_nodes = candidates.select { |node| node.ping_time <= max_latency }
        near_nodes.shuffle!
      end

      def match_tag_sets(candidates)
        matches = []
        tag_sets.find do |tag_set|
          matches = candidates.select { |node| node.matches_tags?(tag_set) }
          !matches.empty?
        end
        matches || []
      end
    end
  end
end
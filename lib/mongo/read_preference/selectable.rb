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
     module Selectable

      attr_reader :tag_sets
      attr_reader :acceptable_latency

      def initialize(tag_sets = [], acceptable_latency = 15)
        raise Exception, "Read preference #{name} cannot be combined " +
            " with tags" if !tag_sets.empty? && !tags_allowed?
        @tag_sets = tag_sets
        @acceptable_latency = acceptable_latency
      end

      def ==(other)
        name == other.name &&
            tag_sets == other.tag_sets &&
            acceptable_latency == other.acceptable_latency
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
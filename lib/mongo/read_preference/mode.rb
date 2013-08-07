# Copyright (C) 2013 10gen Inc.
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
  module ReadPreference
    class Mode
      attr_reader :tag_sets, :acceptable_latency

      def initialize(tag_sets = [], acceptable_latency = 15)
        @tag_sets = tag_sets
        @acceptable_latency = acceptable_latency
      end

      def hash
        [name, tag_sets, acceptable_latency].hash
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
        matching_nodes = match_tag_set(matching_nodes) unless tag_sets.empty?
        matching_nodes
      end

      def near(candidates)
        if !candidates.empty?
          nearest_node = candidates.min_by(&:ping_time)
          max_latency = nearest_node.ping_time + acceptable_latency
          near_nodes = candidates.select do |candidate|
            candidate.ping_time <= max_latency
          end
        else
          near_nodes = []
        end
        near_nodes.shuffle!
      end

      def match_tag_set(candidates)
        matches = []

        # find the first tag_set that has at least 1 match
        tag_sets.find do |tag_set|

          # build an array of nodes that match this tag set
          matches = candidates.select do |candidate|
            candidate.matches_tags?(tag_set)
          end

          !matches.empty?
        end

        matches || []
      end
    end
  end
end

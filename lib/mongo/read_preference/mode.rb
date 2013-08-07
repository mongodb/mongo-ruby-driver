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

    # Base class providing functionality required to represent read preference
    # according to the MongoDB read semantics.
    #
    # @example Use a read preference to select nodes from a list of candidates.
    #   matching_nodes = Mode.select_nodes(nodes)
    class Mode

      # @return [Array<Hash>] The tag sets for this read preference.
      attr_reader :tag_sets

      # @return [Fixnum] The acceptable latency for the read preference.
      attr_reader :acceptable_latency

      # Creates an instance of a read preference mode.
      #
      # @param tag_sets [Array<Hash>] The tag sets.
      # @param acceptable_latency [Fixnum] The acceptable latency.
      def initialize(tag_sets = [], acceptable_latency = 15)
        @tag_sets = tag_sets
        @acceptable_latency = acceptable_latency
      end

      # Hash for this particular read preference mode.
      #
      # @return [Fixnum] The hash of mode name, tag_sets, acceptable latency.
      def hash
        [name, tag_sets, acceptable_latency].hash
      end

      # Tests for read preference equality.
      #
      # @param other [Mongo::ReadPreference::Mode] The other read preference.
      #
      # @return [true, false] The equality of the read preferences.
      def ==(other)
        name == other.name &&
          tag_sets == other.tag_sets &&
          acceptable_latency == other.acceptable_latency
      end

      private

      # Selects primary nodes from a list of candidates.
      #
      # @param candidates [Array<Mongo::Node>] The candidates.
      #
      # @return [Array<Mongo::Node>] Matching primary candidates.
      def primary(candidates)
        candidates.select(&:primary?)
      end

      # Selects secondary nodes from a list of candidates.
      #
      # @param candidates [Array<Mongo::Node>] The candidates.
      #
      # @return [Array<Mongo::Node>] Matching secondary candidates.
      def secondaries(candidates)
        matching_nodes = candidates.select(&:secondary?)
        matching_nodes = match_tag_sets(matching_nodes) unless tag_sets.empty?
        matching_nodes
      end

      # Selects all near nodes from the list of candidates.
      #
      # Near is defined to be within the accpetable latency of the nearest node.
      #
      # @param candidates [Array<Mongo::Node>] The candidates.
      #
      # @return [Array<Mongo::Node>] Near candidates.
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

      # Filters all non tag set matching nodes from the list of candidates.
      #
      # The process by which the nodes are selected is to iterate over the array
      # of tag sets and find the first one of those sets which matches one or
      # more candidate nodes. All candidates that match this tag will be
      # returned in the array. If no tags sets match any node an empty array is
      # returned.
      #
      # @note Logic for determining if the node matches a particular set of
      # tags is defined on the #matches_tags? method of the Node class.
      #
      # @param candidates [Array<Mongo::Node>] The candidates.
      #
      # @return [Array<Mongo::Node>] Candidates that the tag sets.
      def match_tag_sets(candidates)
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

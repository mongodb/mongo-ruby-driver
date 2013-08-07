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

    # Class containing the logic for the Nearest mode.
    #
    # Allows reads from any near member.
    #
    # The Nearest mode places an emphasis on latency in the node selection
    # process and selects nodes from the array of available candidates first
    # by determining if they match the tag sets (if provided), then by
    # filtering matching nodes based upon their latency respective to the
    # matching remaining candidates. If no tag sets are specified, all nodes
    # match leaving latency to determine their selection.
    class Nearest < Mode

      # Name of the mode as a symbol.
      #
      # @return [Symbol] The name of the mode.
      def name
        :nearest
      end

      # Converts this read preference mode instance into a format compatible
      # with mongos.
      #
      # @return [Hash] The read preference for mongos.
      def to_mongos
        read_preference = { :mode => 'nearest' }
        read_preference.merge!({ :tags => tag_sets }) unless tag_sets.empty?
        read_preference
      end

      # Selects nodes for an instance of this read preference mode.
      #
      # @param candidates [Array<Mongo::Node>] The candidates.
      #
      # @return [Array<Mongo::Node>] The selected nodes.
      def select_nodes(candidates)
        if !tag_sets.empty?
          near(match_tag_sets(candidates))
        else
          near(candidates)
        end
      end
    end
  end
end

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

    # Class containing the logic for the Primary mode.
    #
    # Allows reads from primary members only.
    #
    # The Primary mode places an emphasis on consistancy in the node selection
    # process and selects nodes from the array of available candidates by
    # by determining if they are in the primary state.
    class Primary < Mode

      # Name of the mode as a symbol.
      #
      # @return [Symbol] The name of the mode.
      def name
        :primary
      end

      # Whether or not the slave ok bit should be set for this mode.
      #
      # @return [false] The slave ok bit should not be set.
      def slave_ok?
        false
      end

      # Converts this read preference mode instance into a format compatible
      # with mongos.
      #
      # @return [nil] The read preference for mongos.
      def to_mongos
        nil
      end

      # Selects nodes for an instance of this read preference mode.
      #
      # @param candidates [Array<Mongo::Node>] The candidates.
      #
      # @return [Array<Mongo::Node>] The selected node.
      def select_nodes(candidates)
        primary(candidates)
      end
    end
  end
end

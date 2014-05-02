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

    # Encapsulates specifications for selecting near nodes given a list
    #   of candidates.
    #
    # @since 3.0.0
    class Nearest
      include Selectable

      # Get the name of the server mode type.
      #
      # @example Get the name of the server mode for this preference.
      #   preference.name
      #
      # @return [ Symbol ] :nearest
      #
      # @since 3.0.0
      def name
        :nearest
      end

      # Whether the slaveOk bit should be set on wire protocol messages.
      #   I.e. whether the operation can be performed on a secondary node.
      #
      # @return [ true ] true
      #
      # @since 3.0.0
      def slave_ok?
        true
      end

      # Whether tag sets are allowed to be defined for this node preference.
      #
      # @return [ true ] true
      #
      # @since 3.0.0
      def tags_allowed?
        true
      end

      # Convert this node preference definition into a format appropriate
      #   for a mongos server.
      #
      # @example Convert this node preference definition into a format
      #   for mongos.
      #   preference = Mongo::ReadPreference::Nearest.new
      #   preference.to_mongos
      #
      # @return [ Hash ] The node preference formatted for a mongos server.
      #
      # @since 3.0.0
      def to_mongos
        preference = { :mode => 'nearest' }
        preference.merge!({ :tags => tag_sets }) unless tag_sets.empty?
        preference
      end

      # Select the near nodes taking into account any defined tag sets and
      #   acceptable latency between the nearest secondary and other secondaries.
      #
      # @example Select nearest nodes given a list of candidates.
      #   preference = Mongo::ReadPreference::Nearest.new
      #   preference.select_nodes([candidate_1, candidate_2])
      #
      # @return [ Array ] The nearest nodes from the list of candidates.
      #
      # @since 3.0.0
      def select_nodes(candidates)
        if tag_sets.empty?
          # @todo: check to see if candidates should be secondaries only
          near_nodes(candidates)
        else
          near_nodes(match_tag_sets(candidates))
        end
      end
    end
  end
end

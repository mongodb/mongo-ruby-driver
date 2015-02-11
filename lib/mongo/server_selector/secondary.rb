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

    # Encapsulates specifications for selecting secondary servers given a list
    #   of candidates.
    #
    # @since 2.0.0
    class Secondary
      include Selectable

      # Get the name of the server mode type.
      #
      # @example Get the name of the server mode for this preference.
      #   preference.name
      #
      # @return [ Symbol ] :secondary
      #
      # @since 2.0.0
      def name
        :secondary
      end

      # Whether the slaveOk bit should be set on wire protocol messages.
      #   I.e. whether the operation can be performed on a secondary server.
      #
      # @return [ true ] true
      #
      # @since 2.0.0
      def slave_ok?
        true
      end

      # Whether tag sets are allowed to be defined for this server preference.
      #
      # @return [ true ] true
      #
      # @since 2.0.0
      def tags_allowed?
        true
      end

      # Convert this server preference definition into a format appropriate
      #   for a mongos server.
      #
      # @example Convert this server preference definition into a format
      #   for mongos.
      #   preference = Mongo::ServerSelector::Secondary.new
      #   preference.to_mongos
      #
      # @return [ Hash ] The server preference formatted for a mongos server.
      #
      # @since 2.0.0
      def to_mongos
        preference = { :mode => 'secondary' }
        preference.merge!({ :tags => tag_sets }) unless tag_sets.empty?
        preference
      end

      private

      # Select the secondary servers taking into account any defined tag sets and
      #   local threshold between the nearest secondary and other secondaries.
      #
      # @example Select secondary servers given a list of candidates.
      #   preference = Mongo::ServerSelector::Secondary.new
      #   preference.select([candidate_1, candidate_2])
      #
      # @return [ Array ] The secondary servers from the list of candidates.
      #
      # @since 2.0.0
      def select(candidates)
        near_servers(secondaries(candidates))
      end
    end
  end
end

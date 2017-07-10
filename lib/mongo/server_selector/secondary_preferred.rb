# Copyright (C) 2014-2017 MongoDB, Inc.
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

    # Encapsulates specifications for selecting servers, with
    #   secondaries preferred, given a list of candidates.
    #
    # @since 2.0.0
    class SecondaryPreferred
      include Selectable

      # Get the name of the server mode type.
      #
      # @example Get the name of the server mode for this preference.
      #   preference.name
      #
      # @return [ Symbol ] :secondary_preferred
      #
      # @since 2.0.0
      def name
        :secondary_preferred
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
      # Note that the server preference is not sent to mongos as part of the query
      #   selector if there are no tag sets, for maximum backwards compatibility.
      #
      # @example Convert this server preference definition into a format
      #   for mongos.
      #   preference = Mongo::ServerSelector::SecondaryPreferred.new
      #   preference.to_mongos
      #
      # @return [ Hash ] The server preference formatted for a mongos server.
      #
      # @since 2.0.0
      def to_mongos
        return nil if tag_sets.empty? && max_staleness.nil?
        preference = { mode: 'secondaryPreferred' }
        preference.merge!({ tags: tag_sets }) unless tag_sets.empty?
        preference.merge!({ maxStalenessSeconds: max_staleness }) if max_staleness
        preference
      end

      private

      # Select servers taking into account any defined tag sets and
      #   local threshold, with secondaries.
      #
      # @example Select servers given a list of candidates,
      #   with secondaries preferred.
      #   preference = Mongo::ServerSelector::SecondaryPreferred.new
      #   preference.select([candidate_1, candidate_2])
      #
      # @return [ Array ] A list of servers matching tag sets and acceptable
      #   latency with secondaries preferred.
      #
      # @since 2.0.0
      def select(candidates)
        near_servers(secondaries(candidates)) + primary(candidates)
      end

      def max_staleness_allowed?
        true
      end
    end
  end
end

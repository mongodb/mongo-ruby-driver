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

    # Encapsulates specifications for selecting servers, with the
    #   primary preferred, given a list of candidates.
    #
    # @since 3.0.0
    class PrimaryPreferred
      include Selectable

      # Get the name of the server mode type.
      #
      # @example Get the name of the server mode for this preference.
      #   preference.name
      #
      # @return [ Symbol ] :primary_preferred
      #
      # @since 3.0.0
      def name
        :primary_preferred
      end

      # Whether the slaveOk bit should be set on wire protocol messages.
      #   I.e. whether the operation can be performed on a secondary server.
      #
      # @return [ true ] true
      #
      # @since 3.0.0
      def slave_ok?
        true
      end

      # Whether tag sets are allowed to be defined for this server preference.
      #
      # @return [ true ] true
      #
      # @since 3.0.0
      def tags_allowed?
        true
      end

      # Convert this server preference definition into a format appropriate
      #   for a mongos server.
      #
      # @example Convert this server preference definition into a format
      #   for mongos.
      #   preference = Mongo::ServerPreference::PrimaryPreferred.new
      #   preference.to_mongos
      #
      # @return [ Hash ] The server preference formatted for a mongos server.
      #
      # @since 3.0.0
      def to_mongos
        preference = { :mode => 'primaryPreferred' }
        preference.merge!({ :tags => tag_sets }) unless tag_sets.empty?
        preference
      end

      # Select servers taking into account any defined tag sets and
      #   acceptable latency, with the primary preferred.
      #
      # @example Select servers given a list of candidates,
      #   with the primary preferred.
      #   preference = Mongo::ServerPreference::PrimaryPreferred.new
      #   preference.select_servers([candidate_1, candidate_2])
      #
      # @return [ Array ] A list of servers matching tag sets and acceptable
      #   latency with the primary preferred.
      #
      # @since 3.0.0
      def select_servers(candidates)
        primary(candidates) + near_servers(secondaries(candidates))
      end
    end
  end
end

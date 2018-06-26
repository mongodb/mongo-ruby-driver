# Copyright (C) 2014-2018 MongoDB, Inc.
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

    # Encapsulates specifications for selecting near servers given a list
    #   of candidates.
    #
    # @since 2.0.0
    class Nearest
      include Selectable

      # Name of the this read preference in the server's format.
      #
      # @since 2.5.0
      SERVER_FORMATTED_NAME = 'nearest'.freeze

      # Get the name of the server mode type.
      #
      # @example Get the name of the server mode for this preference.
      #   preference.name
      #
      # @return [ Symbol ] :nearest
      #
      # @since 2.0.0
      def name
        :nearest
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
      #   preference = Mongo::ServerSelector::Nearest.new
      #   preference.to_mongos
      #
      # @return [ Hash ] The server preference formatted for a mongos server.
      #
      # @since 2.0.0
      def to_mongos
        @doc ||= (preference = { :mode => SERVER_FORMATTED_NAME }
          preference.merge!({ :tags => tag_sets }) unless tag_sets.empty?
          preference.merge!({ maxStalenessSeconds: max_staleness }) if max_staleness
          preference)
      end
      alias :to_doc :to_mongos

      private

      # Select the near servers taking into account any defined tag sets and
      #   local threshold between the nearest server and other servers.
      #
      # @example Select nearest servers given a list of candidates.
      #   preference = Mongo::ServerSelector::Nearest.new
      #   preference.select_server(cluster)
      #
      # @return [ Array ] The nearest servers from the list of candidates.
      #
      # @since 2.0.0
      def select(candidates)
        matching_servers = filter_stale_servers(candidates, primary(candidates).first)
        matching_servers = match_tag_sets(matching_servers) unless tag_sets.empty?
        near_servers(matching_servers)
      end

      def max_staleness_allowed?
        true
      end
    end
  end
end

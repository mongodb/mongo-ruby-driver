# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2014-2020 MongoDB Inc.
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
    class Secondary < Base

      # Name of the this read preference in the server's format.
      #
      # @since 2.5.0
      SERVER_FORMATTED_NAME = 'secondary'.freeze

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

      # Whether the secondaryOk bit should be set on wire protocol messages.
      #   I.e. whether the operation can be performed on a secondary server.
      #
      # @return [ true ] true
      # @api private
      def secondary_ok?
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

      # Whether the hedge option is allowed to be defined for this server preference.
      #
      # @return [ true ] true
      def hedge_allowed?
        true
      end

      # Convert this server preference definition into a format appropriate
      #   for sending to a MongoDB server (i.e., as a command field).
      #
      # @return [ Hash ] The server preference formatted as a command field value.
      #
      # @since 2.0.0
      def to_doc
        full_doc
      end

      # Convert this server preference definition into a value appropriate
      #   for sending to a mongos.
      #
      # This method may return nil if the read preference should not be sent
      # to a mongos.
      #
      # @return [ Hash | nil ] The server preference converted to a mongos
      #   command field value.
      #
      # @since 2.0.0
      alias :to_mongos :to_doc

      private

      # Select the secondary servers taking into account any defined tag sets and
      #   local threshold between the nearest secondary and other secondaries.
      #
      # @return [ Array ] The secondary servers from the list of candidates.
      #
      # @since 2.0.0
      def select_in_replica_set(candidates)
        near_servers(secondaries(candidates))
      end

      def max_staleness_allowed?
        true
      end
    end
  end
end

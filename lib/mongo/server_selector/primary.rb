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

    # Encapsulates specifications for selecting the primary server given a list
    #   of candidates.
    #
    # @since 2.0.0
    class Primary < Base

      # Name of the this read preference in the server's format.
      #
      # @since 2.5.0
      SERVER_FORMATTED_NAME = 'primary'.freeze

      # Get the name of the server mode type.
      #
      # @example Get the name of the server mode for this preference.
      #   preference.name
      #
      # @return [ Symbol ] :primary
      #
      # @since 2.0.0
      def name
        :primary
      end

      # Whether the secondaryOk bit should be set on wire protocol messages.
      #   I.e. whether the operation can be performed on a secondary server.
      #
      # @return [ false ] false
      # @api private
      def secondary_ok?
        false
      end

      # Whether tag sets are allowed to be defined for this server preference.
      #
      # @return [ false ] false
      #
      # @since 2.0.0
      def tags_allowed?
        false
      end

      # Whether the hedge option is allowed to be defined for this server preference.
      #
      # @return [ false ] false
      def hedge_allowed?
        false
      end

      # Convert this server preference definition into a format appropriate
      #   for sending to a MongoDB server (i.e., as a command field).
      #
      # @return [ Hash ] The server preference formatted as a command field value.
      #
      # @since 2.5.0
      def to_doc
        { mode: SERVER_FORMATTED_NAME }
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
      def to_mongos
        nil
      end

      private

      # Select the primary server from a list of candidates.
      #
      # @return [ Array ] The primary server from the list of candidates.
      #
      # @since 2.0.0
      def select_in_replica_set(candidates)
        primary(candidates)
      end

      def max_staleness_allowed?
        false
      end
    end
  end
end

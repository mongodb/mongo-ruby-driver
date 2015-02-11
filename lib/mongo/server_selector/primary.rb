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

    # Encapsulates specifications for selecting the primary server given a list
    #   of candidates.
    #
    # @since 2.0.0
    class Primary
      include Selectable

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

      # Whether the slaveOk bit should be set on wire protocol messages.
      #   I.e. whether the operation can be performed on a secondary server.
      #
      # @return [ false ] false
      #
      # @since 2.0.0
      def slave_ok?
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

      # Convert this server preference definition into a format appropriate
      #   for a mongos server.
      #
      # @example Convert this server preference definition into a format
      #   for mongos.
      #   preference = Mongo::ServerSelector::Primary.new
      #   preference.to_mongos
      #
      # @return [ nil ] nil
      #
      # @since 2.0.0
      def to_mongos
        nil
      end

      private

      # Select the primary server from a list of candidates.
      #
      # @example Select the primary server given a list of candidates.
      #   preference = Mongo::ServerSelector::Primary.new
      #   preference.select([candidate_1, candidate_2])
      #
      # @return [ Array ] The primary server from the list of candidates.
      #
      # @since 2.0.0
      def select(candidates)
        primary(candidates)
      end
    end
  end
end

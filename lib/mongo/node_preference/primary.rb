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

    # Encapsulates specifications for selecting the primary node given a list
    #   of candidates.
    #
    # @since 3.0.0
    class Primary
      include Selectable

      # Get the name of the server mode type.
      #
      # @example Get the name of the server mode for this preference.
      #   preference.name
      #
      # @return [ Symbol ] :primary
      #
      # @since 3.0.0
      def name
        :primary
      end

      # Whether the slaveOk bit should be set on wire protocol messages.
      #   I.e. whether the operation can be performed on a secondary node.
      #
      # @return [ false ] false
      #
      # @since 3.0.0
      def slave_ok?
        false
      end

      # Whether tag sets are allowed to be defined for this node preference.
      #
      # @return [ false ] false
      #
      # @since 3.0.0
      def tags_allowed?
        false
      end

      # Convert this node preference definition into a format appropriate
      #   for a mongos server.
      #
      # @example Convert this node preference definition into a format
      #   for mongos.
      #   preference = Mongo::ReadPreference::Primary.new
      #   preference.to_mongos
      #
      # @return [ nil ] nil
      #
      # @since 3.0.0
      def to_mongos
        nil
      end

      # Select the primary node from a list of candidates.
      #
      # @example Select the primary node given a list of candidates.
      #   preference = Mongo::ReadPreference::Primary.new
      #   preference.select_nodes([candidate_1, candidate_2])
      #
      # @return [ Array ] The primary node from the list of candidates.
      #
      # @since 3.0.0
      def select_nodes(candidates)
        primary(candidates)
      end
    end
  end
end

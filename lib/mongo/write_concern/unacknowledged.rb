# Copyright (C) 2009-2014 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

module Mongo
  module WriteConcern

    # An unacknowledged write concern will provide no error on write outside of
    # network and connection exceptions.
    #
    # @since 2.0.0
    class Unacknowledged < Mode

      # The noop constant for the gle.
      #
      # @since 2.0.0
      NOOP = nil

      # Get the gle command for an unacknowledged write.
      #
      # @example Get the gle command.
      #   unacknowledged.get_last_error
      #
      # @return [ nil ] The noop.
      #
      # @since 2.0.0
      def get_last_error
        NOOP
      end
    end
  end
end

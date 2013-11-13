# Copyright (C) 2009-2013 MongoDB, Inc.
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

    # An acknowledged write concern provides a get last error command with the
    # appropriate options on each write operation.
    #
    # @since 2.0.0
    class Acknowledged < Mode

      # Get the get last error command for the concern.
      #
      # @example Get the gle command.
      #   acknowledged.get_last_error
      #
      # @return [ Hash ] The gle command.
      #
      # @since 2.0.0
      def get_last_error
        @get_last_error ||= { :getlasterror => 1 }.merge(normalize(options))
      end
    end
  end
end

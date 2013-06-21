# Copyright (C) 2013 10gen Inc.
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
  module Protocol

    # MongoDB Wire protocol KillCursors message.
    #
    # This is a client request message that is sent to the server in order
    # to kill a number of cursors.
    #
    # @api semipublic
    class KillCursors < Message

      # Creates a new KillCursors message
      #
      # @example Get more results using the database default limit.
      #   KillCursors.new([])
      #
      # @param cursor_ids [Array<Fixnum>] The cursor ids to kill.
      # @param options [Hash] The additional kill cursors options.
      def initialize(cursor_ids, options = {})
        @cursor_ids = cursor_ids
        @id_count   = @cursor_ids.size
      end

      private

      # The operation code required to specify KillCursors message.
      # @return [Fixnum] the operation code.
      def op_code
        2007
      end

      # Field representing Zero encoded as an Int32.
      field :zero, Zero

      # @!attribute
      # @return [Fixnum] Count of the nubmer of cursor ids.
      field :id_count, Int32

      # @!attribute
      # @return [Array<Fixnum>] Cursors to kill.
      field :cursor_ids, Int64, :multi => true
    end
  end
end

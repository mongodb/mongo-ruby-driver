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
      # @example Kill the cursor on the server with id 1.
      #   KillCursors.new([1])
      #
      # @param [ Mongo::Database ] collection The collection.
      # @param [ Mongo::Database ] database The database.
      # @param [ Array<Fixnum> ] cursor_ids The cursor ids to kill.
      def initialize(collection, database, cursor_ids)
        @database = database
        @cursor_ids = cursor_ids
        @id_count   = @cursor_ids.size
        @upconverter = Upconverter.new(collection, cursor_ids)
      end

      # Return the event payload for monitoring.
      #
      # @example Return the event payload.
      #   message.payload
      #
      # @return [ Hash ] The event payload.
      #
      # @since 2.1.0
      def payload
        {
          command_name: 'killCursors',
          database_name: @database,
          command: upconverter.command,
          request_id: request_id
        }
      end

      private

      attr_reader :upconverter

      # The operation code required to specify +KillCursors+ message.
      # @return [Fixnum] the operation code.
      def op_code
        2007
      end

      # Field representing Zero encoded as an Int32.
      field :zero, Zero

      # @!attribute
      # @return [Fixnum] Count of the number of cursor ids.
      field :id_count, Int32

      # @!attribute
      # @return [Array<Fixnum>] Cursors to kill.
      field :cursor_ids, Int64, true

      # Converts legacy insert messages to the appropriare OP_COMMAND style
      # message.
      #
      # @since 2.1.0
      class Upconverter

        # The kill cursors constant.
        #
        # @since 2.2.0
        KILL_CURSORS = 'killCursors'.freeze

        # The cursors constant.
        #
        # @since 2.2.0
        CURSORS = 'cursors'.freeze

        # @return [ String ] collection The name of the collection.
        attr_reader :collection

        # @return [ Array<Integer> ] cursor_ids The cursor ids.
        attr_reader :cursor_ids

        # Instantiate the upconverter.
        #
        # @example Instantiate the upconverter.
        #   Upconverter.new('users', [ 1, 2, 3 ])
        #
        # @param [ String ] collection The name of the collection.
        # @param [ Array<Integer> ] cursor_ids The cursor ids.
        #
        # @since 2.1.0
        def initialize(collection, cursor_ids)
          @collection = collection
          @cursor_ids = cursor_ids
        end

        # Get the upconverted command.
        #
        # @example Get the command.
        #   upconverter.command
        #
        # @return [ BSON::Document ] The upconverted command.
        #
        # @since 2.1.0
        def command
          document = BSON::Document.new
          document.store(KILL_CURSORS, collection)
          document.store(CURSORS, cursor_ids)
          document
        end
      end
    end
  end
end

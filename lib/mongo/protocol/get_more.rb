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

    # MongoDB Wire protocol GetMore message.
    #
    # This is a client request message that is sent to the server in order
    # to retrieve additional documents from a cursor that has already been
    # instantiated.
    #
    # The operation requires that you specify the database and collection
    # name as well as the cursor id because cursors are scoped to a namespace.
    #
    # @api semipublic
    class GetMore < Message

      # Creates a new GetMore message
      #
      # @example Get 15 additional documents from cursor 123 in 'xgen.users'.
      #   GetMore.new('xgen', 'users', 15, 123)
      #
      # @param database [String, Symbol] The database to query.
      # @param collection [String, Symbol] The collection to query.
      # @param number_to_return [Integer] The number of documents to return.
      # @param cursor_id [Integer] The cursor id returned in a reply.
      def initialize(database, collection, number_to_return, cursor_id)
        @namespace = "#{database}.#{collection}"
        @number_to_return = number_to_return
        @cursor_id = cursor_id
      end

      # The log message for a get more operation.
      #
      # @example Get the log message.
      #   get_more.log_message
      #
      # @return [ String ] The log message
      #
      # @since 2.0.0
      def log_message
        fields = []
        fields << ["%s |", "GETMORE"]
        fields << ["namespace=%s", namespace]
        fields << ["number_to_return=%s", number_to_return]
        fields << ["cursor_id=%s", cursor_id]
        f, v = fields.transpose
        f.join(" ") % v
      end

      # Get more messages require replies from the database.
      #
      # @example Does the message require a reply?
      #   message.replyable?
      #
      # @return [ true ] Always true for get more.
      #
      # @since 2.0.0
      def replyable?
        true
      end

      private

      # The operation code required to specify a GetMore message.
      # @return [Fixnum] the operation code.
      def op_code
        2005
      end

      # Field representing Zero encoded as an Int32
      field :zero, Zero

      # @!attribute
      # @return [String] The namespace for this GetMore message.
      field :namespace, CString

      # @!attribute
      # @return [Fixnum] The number to return for this GetMore message.
      field :number_to_return, Int32

      # @!attribute
      # @return [Fixnum] The cursor id to get more documents from.
      field :cursor_id, Int64
    end
  end
end

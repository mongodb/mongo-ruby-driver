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
  module Protocol

    # MongoDB Wire protocol getMore message.
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

      # Creates a new getMore message
      #
      # @example Get 15 additional documents from cursor 123 in 'xgen.users'.
      #   GetMore.new('xgen', 'users', 15, 123)
      #
      # @param database [String, Symbol] The database to query.
      # @param collection [String, Symbol] The collection to query.
      # @param number_to_return [Integer] The number of documents to return.
      # @param cursor_id [Integer] The cursor id returned in a reply.
      def initialize(database, collection, number_to_return, cursor_id)
        @database = database
        @namespace = "#{database}.#{collection}"
        @number_to_return = number_to_return
        @cursor_id = cursor_id
        @upconverter = Upconverter.new(collection, cursor_id, number_to_return)
        super
      end

      # Return the event payload for monitoring.
      #
      # @example Return the event payload.
      #   message.payload
      #
      # @return [ BSON::Document ] The event payload.
      #
      # @since 2.1.0
      def payload
        BSON::Document.new(
          command_name: 'getMore',
          database_name: @database,
          command: upconverter.command,
          request_id: request_id
        )
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

      protected

      attr_reader :upconverter

      private

      # The operation code required to specify a getMore message.
      # @return [Fixnum] the operation code.
      #
      # @since 2.5.0
      OP_CODE = 2005

      # Field representing Zero encoded as an Int32
      field :zero, Zero

      # @!attribute
      # @return [String] The namespace for this getMore message.
      field :namespace, CString

      # @!attribute
      # @return [Fixnum] The number to return for this getMore message.
      field :number_to_return, Int32

      # @!attribute
      # @return [Fixnum] The cursor id to get more documents from.
      field :cursor_id, Int64

      # Converts legacy getMore messages to the appropriare OP_COMMAND style
      # message.
      #
      # @since 2.1.0
      class Upconverter

        # The get more constant.
        #
        # @since 2.2.0
        # @deprecated
        GET_MORE = 'getMore'.freeze

        # @return [ String ] collection The name of the collection.
        attr_reader :collection

        # @return [ Integer ] cursor_id The cursor id.
        attr_reader :cursor_id

        # @return [ Integer ] number_to_return The number of docs to return.
        attr_reader :number_to_return

        # Instantiate the upconverter.
        #
        # @example Instantiate the upconverter.
        #   Upconverter.new('users', 1, 1)
        #
        # @param [ String ] collection The name of the collection.
        # @param [ Integer ] cursor_id The cursor id.
        # @param [ Integer ] number_to_return The number of documents to
        #   return.
        #
        # @since 2.1.0
        def initialize(collection, cursor_id, number_to_return)
          @collection = collection
          @cursor_id = cursor_id
          @number_to_return = number_to_return
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
          document.store('getMore', BSON::Int64.new(cursor_id))
          document.store(Message::BATCH_SIZE, number_to_return)
          document.store(Message::COLLECTION, collection)
          document
        end
      end

      Registry.register(OP_CODE, self)
    end
  end
end

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

    # The MongoDB wire protocol message representing a reply
    #
    # @example
    #   socket = TCPSocket.new('localhost', 27017)
    #   query = Protocol::Query.new('xgen', 'users', {:name => 'Tyler'})
    #   socket.write(query)
    #   reply = Protocol::Reply::deserialize(socket)
    #
    # @api semipublic
    class Reply < Message

      # Determine if the reply had a query failure flag.
      #
      # @example Did the reply have a query failure.
      #   reply.query_failure?
      #
      # @return [ true, false ] If the query failed.
      #
      # @since 2.0.5
      def query_failure?
        flags.include?(:query_failure)
      end

      # Determine if the reply had a cursor not found flag.
      #
      # @example Did the reply have a cursor not found flag.
      #   reply.cursor_not_found?
      #
      # @return [ true, false ] If the query cursor was not found.
      #
      # @since 2.2.3
      def cursor_not_found?
        flags.include?(:cursor_not_found)
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
          reply: upconverter.command,
          request_id: request_id
        )
      end

      private

      def upconverter
        @upconverter ||= Upconverter.new(documents, cursor_id, starting_from)
      end

      # The operation code required to specify a Reply message.
      # @return [Fixnum] the operation code.
      #
      # @since 2.5.0
      OP_CODE = 1

      # Available flags for a Reply message.
      FLAGS = [
        :cursor_not_found,
        :query_failure,
        :shard_config_stale,
        :await_capable
      ]

      public

      # @!attribute
      # @return [Array<Symbol>] The flags for this reply.
      #
      #   Supported flags: +:cursor_not_found+, +:query_failure+,
      #   +:shard_config_stale+, +:await_capable+
      field :flags, BitVector.new(FLAGS)

      # @!attribute
      # @return [Fixnum] The cursor id for this response. Will be zero
      #   if there are no additional results.
      field :cursor_id, Int64

      # @!attribute
      # @return [Fixnum] The starting position of the cursor for this Reply.
      field :starting_from, Int32

      # @!attribute
      # @return [Fixnum] Number of documents in this Reply.
      field :number_returned, Int32

      # @!attribute
      # @return [Array<Hash>] The documents in this Reply.
      field :documents, Document, :@number_returned

      # Upconverts legacy replies to new op command replies.
      #
      # @since 2.1.0
      class Upconverter

        # Next batch constant.
        #
        # @since 2.1.0
        NEXT_BATCH = 'nextBatch'.freeze

        # First batch constant.
        #
        # @since 2.1.0
        FIRST_BATCH = 'firstBatch'.freeze

        # Cursor field constant.
        #
        # @since 2.1.0
        CURSOR = 'cursor'.freeze

        # Id field constant.
        #
        # @since 2.1.0
        ID = 'id'.freeze

        # Initialize the new upconverter.
        #
        # @example Create the upconverter.
        #   Upconverter.new(docs, 1, 3)
        #
        # @param [ Array<BSON::Document> ] documents The documents.
        # @param [ Integer ] cursor_id The cursor id.
        # @param [ Integer ] starting_from The starting position.
        #
        # @since 2.1.0
        def initialize(documents, cursor_id, starting_from)
          @documents = documents
          @cursor_id = cursor_id
          @starting_from = starting_from
        end

        # @return [ Array<BSON::Document> ] documents The documents.
        attr_reader :documents

        # @return [ Integer ] cursor_id The cursor id.
        attr_reader :cursor_id

        # @return [ Integer ] starting_from The starting point in the cursor.
        attr_reader :starting_from

        # Get the upconverted command.
        #
        # @example Get the command.
        #   upconverter.command
        #
        # @return [ BSON::Document ] The command.
        #
        # @since 2.1.0
        def command
          command? ? op_command : find_command
        end

        private

        def batch_field
          starting_from > 0 ? NEXT_BATCH : FIRST_BATCH
        end

        def command?
          !documents.empty? && documents.first.key?(Operation::Result::OK)
        end

        def find_command
          document = BSON::Document.new
          cursor_document = BSON::Document.new
          cursor_document.store(ID, cursor_id)
          cursor_document.store(batch_field, documents)
          document.store(Operation::Result::OK, 1)
          document.store(CURSOR, cursor_document)
          document
        end

        def op_command
          documents.first
        end
      end

      Registry.register(OP_CODE, self)
    end
  end
end

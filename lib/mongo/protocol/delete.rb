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

    # MongoDB Wire protocol Delete message.
    #
    # This is a client request message that is sent to the server in order
    # to delete selected documents in the specified namespace.
    #
    # The operation, by default, operates on many documents. Setting
    # the +:single_remove+ flag allows for a single matching document
    # to be removed.
    #
    # @api semipublic
    class Delete < Message

      # Creates a new Delete message
      #
      # @example Remove all users named Tyler.
      #   Query.new('xgen', 'users', {:name => 'Tyler'})
      #
      # @param database [String, Symbol] The database to remove from.
      # @param collection [String, Symbol] The collection to remove from.
      # @param selector [Hash] The query used to select doc(s) to remove.
      # @param options [Hash] The additional delete options.
      #
      # @option options :flags [Array] The flags for the delete message.
      #
      #   Supported flags: +:single_remove+
      def initialize(database, collection, selector, options = {})
        @database = database
        @namespace = "#{database}.#{collection}"
        @selector = selector
        @flags = options[:flags] || []
        @upconverter = Upconverter.new(collection, selector, options)
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
          command_name: 'delete',
          database_name: @database,
          command: upconverter.command,
          request_id: request_id
        }
      end

      private

      attr_reader :upconverter

      # The operation code required to specify a Delete message.
      # @return [Fixnum] the operation code.
      def op_code
        2006
      end

      # Available flags for a Delete message.
      FLAGS = [:single_remove]

      # Field representing Zero encoded as an Int32.
      field :zero, Zero

      # @!attribute
      # @return [String] The namespace for this Delete message.
      field :namespace, CString

      # @!attribute
      # @return [Array<Symbol>] The flags for this Delete message.
      field :flags, BitVector.new(FLAGS)

      # @!attribute
      # @return [Hash] The selector for this Delete message.
      field :selector, Document

      # Converts legacy delete messages to the appropriare OP_COMMAND style
      # message.
      #
      # @since 2.1.0
      class Upconverter

        # The delete command constant.
        #
        # @since 2.2.0
        DELETE = 'delete'.freeze

        # The deletes command constant.
        #
        # @since 2.2.0
        DELETES = 'deletes'.freeze

        # @return [ String ] collection The name of the collection.
        attr_reader :collection

        # @return [ BSON::Document, Hash ] filter The query filter or command.
        attr_reader :filter

        # @return [ Hash ] options The options.
        attr_reader :options

        # Instantiate the upconverter.
        #
        # @example Instantiate the upconverter.
        #   Upconverter.new('users', { name: 'test' })
        #
        # @param [ String ] collection The name of the collection.
        # @param [ BSON::Document, Hash ] filter The filter or command.
        #
        # @since 2.1.0
        def initialize(collection, filter, options)
          @collection = collection
          @filter = filter
          @options = options
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
          document.store(DELETE, collection)
          document.store(DELETES, [ BSON::Document.new(Message::Q => filter, Message::LIMIT => limit) ])
          document.store(Message::ORDERED, true)
          document
        end

        private

        def limit
          if options.key?(:flags)
            options[:flags].include?(:single_remove) ? 1 : 0
          else
            0
          end
        end
      end
    end
  end
end

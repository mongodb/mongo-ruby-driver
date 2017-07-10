# Copyright (C) 2014-2017 MongoDB, Inc.
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

    # MongoDB Wire protocol Update message.
    #
    # This is a client request message that is sent to the server in order
    # to update documents matching the provided query.
    #
    # The default is to update a single document. In order to update many at
    # a time users should set the +:multi_update+ flag for the update.
    #
    # If an upsert (update or insert) is desired, users can set the +:upsert+
    # flag in order to indicate they would like to insert the merged selector
    # and update if no document matching the update query currently exists.
    #
    # @api semipublic
    class Update < Message

      # Creates a new Update message
      #
      # @example Update single document
      #   Update.new('xgen', 'users', {:name => 'Tyler'}, {:name => 'Bob'})
      #
      # @example Perform a multi update
      #   Update.new('xgen', 'users',
      #     {:age => 20}, {:age => 21}, :flags => [:multi_update])
      #
      # @example Perform an upsert
      #   Update.new('xgen', 'users', {:name => 'Tyler'}, :flags => [:upsert])
      #
      # @param database [String, Symbol]  The database to update.
      # @param collection [String, Symbol] The collection to update.
      # @param selector [Hash] The update selector.
      # @param update [Hash] The update to perform.
      # @param options [Hash] The additional query options.
      #
      # @option options :flags [Array] The flags for the update message.
      #
      #   Supported flags: +:upsert+, +:multi_update+
      def initialize(database, collection, selector, update, options = {})
        @database    = database
        @collection  = collection
        @namespace   = "#{database}.#{collection}"
        @selector    = selector
        @update      = update
        @flags       = options[:flags] || []
        @upconverter = Upconverter.new(collection, selector, update, flags)
        super
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
          command_name: 'update',
          database_name: @database,
          command: upconverter.command,
          request_id: request_id
        }
      end

      protected

      attr_reader :upconverter

      private

      # The operation code required to specify an Update message.
      # @return [Fixnum] the operation code.
      def op_code
        2001
      end

      # Available flags for an Update message.
      FLAGS = [:upsert, :multi_update]

      # Field representing Zero encoded as an Int32.
      field :zero, Zero

      # @!attribute
      # @return [String] The namespace for this Update message.
      field :namespace, CString

      # @!attribute
      # @return [Array<Symbol>] The flags for this Update message.
      field :flags, BitVector.new(FLAGS)

      # @!attribute
      # @return [Hash] The selector for this Update message.
      field :selector, Document

      # @!attribute
      # @return [Hash] The update for this Delete message.
      field :update, Document

      # Converts legacy update messages to the appropriare OP_COMMAND style
      # message.
      #
      # @since 2.1.0
      class Upconverter

        # The multi constant.
        #
        # @since 2.2.0
        MULTI = 'multi'.freeze

        # The u constant.
        #
        # @since 2.2.0
        U = 'u'.freeze

        # The update constant.
        #
        # @since 2.2.0
        UPDATE = 'update'.freeze

        # The updates constant.
        #
        # @since 2.2.0
        UPDATES = 'updates'.freeze

        # The upsert constant.
        #
        # @since 2.2.0
        UPSERT = 'upsert'.freeze

        # @return [ String ] collection The name of the collection.
        attr_reader :collection

        # @return [ Hash ] filter The filter.
        attr_reader :filter

        # @return [ Hash ] update The update.
        attr_reader :update

        # @return [ Array<Symbol> ] flags The flags.
        attr_reader :flags

        # Instantiate the upconverter.
        #
        # @example Instantiate the upconverter.
        #   Upconverter.new(
        #     'users',
        #     { name: 'test' },
        #     { '$set' => { 'name' => 't' }},
        #     []
        #   )
        #
        # @param [ String ] collection The name of the collection.
        # @param [ Hash ] filter The filter.
        # @param [ Hash ] update The update.
        # @param [ Array<Symbol> ] flags The flags.
        #
        # @since 2.1.0
        def initialize(collection, filter, update, flags)
          @collection = collection
          @filter = filter
          @update = update
          @flags = flags
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
          updates = BSON::Document.new
          updates.store(Message::Q, filter)
          updates.store(U, update)
          updates.store(MULTI, flags.include?(:multi_update))
          updates.store(UPSERT, flags.include?(:upsert))
          document.store(UPDATE, collection)
          document.store(Message::ORDERED, true)
          document.store(UPDATES, [ updates ])
          document
        end
      end
    end
  end
end

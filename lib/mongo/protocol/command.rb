# Copyright (C) 2014-2016 MongoDB, Inc.
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

    # MongoDB Wire protocol Query message, specifically a command.
    #
    # The collection against which this 'query' is run is always
    #   '$cmd'.
    #
    # @api semipublic
    class Command < Message

      # Creates a new Query message
      #
      # @example A message for the create command.
      #   Command.new('music', '$cmd', {:create => 'bands'})
      #
      # @param database [ String, Symbol ] The database on which this command is run.
      # @param collection [ String, Symbol ] The collection, which is always $cmd. This argument
      #   is preserved for consistency with other Protocol message APIs.
      # @param selector [ Hash ] The command document.
      # @param options [ Hash ] The additional query options.
      #
      # Supported flags: +:slave_ok
      #
      # @since 2.4.0
      def initialize(database, collection, selector, options = {})
        @database = database
        @namespace = "#{database}.#{Database::COMMAND}"
        @selector = selector
        @options = options
        @project = nil
        @limit = -1
        @skip = 0
        @flags = options[:flags] || []
        @upconverter = Upconverter.new(collection, selector, options, flags)
        super
      end

      # Return the event payload for monitoring.
      #
      # @example Return the event payload.
      #   message.payload
      #
      # @return [ Hash ] The event payload.
      #
      # @since 2.4.0
      def payload
        {
          command_name: upconverter.command_name,
          database_name: @database,
          command: upconverter.command,
          request_id: request_id
        }
      end

      # Command (Query) messages require replies from the database.
      #
      # @example Does the message require a reply?
      #   message.replyable?
      #
      # @return [ true ] Always true for queries/commands.
      #
      # @since 2.4.0
      def replyable?
        true
      end

      protected

      attr_reader :upconverter

      private

      def extra_doc_size
        1024 * 16
      end

      # The operation code required to specify a Query Command message.
      # @return [Fixnum] the operation code.
      def op_code
        2004
      end

      # Available flags for a Query/Command message.
      FLAGS = [
        :reserved,
        :tailable_cursor,
        :slave_ok,
        :oplog_replay,
        :no_cursor_timeout,
        :await_data,
        :exhaust,
        :partial
      ]

      # @!attribute
      # @return [Array<Symbol>] The flags for this query command message.
      field :flags, BitVector.new(FLAGS)

      # @!attribute
      # @return [String] The namespace for this query command message.
      field :namespace, CString

      # @!attribute
      # @return [Integer]
      field :skip, Int32

      # @!attribute
      # @return [Integer]
      field :limit, Int32

      # @!attribute
      # @return [Hash] The command document.
      field :selector, Document

      # @!attribute
      # @return [Hash]
      field :project, Document

      # Provides structured information about the command.
      #
      # @since 2.4.0
      class Upconverter

        # Find command constant.
        #
        # @since 2.4.0
        FIND = 'find'.freeze

        # @return [ String ] collection The name of the collection.
        attr_reader :collection

        # @return [ BSON::Document, Hash ] filter The query filter / command.
        attr_reader :filter

        # @return [ BSON::Document, Hash ] options The options.
        attr_reader :options

        # @return [ Array<Symbol> ] flags The flags.
        attr_reader :flags

        # Instantiate the upconverter.
        #
        # @example Instantiate the upconverter.
        #   Upconverter.new('users', { name: 'test' }, { skip: 10 })
        #
        # @param [ String ] collection The name of the collection.
        # @param [ BSON::Document, Hash ] filter The filter / command.
        # @param [ BSON::Document, Hash ] options The options.
        # @param [ Array<Symbol> ] flags The flags.
        #
        # @since 2.4.0
        def initialize(collection, filter, options, flags)
          @collection = collection
          @filter = filter
          @options = options
          @flags = flags
        end

        # Get the upconverted command.
        #
        # @example Get the command.
        #   upconverter.command
        #
        # @return [ BSON::Document ] The upconverted command.
        #
        # @since 2.4.0
        def command
          document = BSON::Document.new
          (filter[:$query] || filter).each do |field, value|
            document.store(field.to_s, value)
          end
          document
        end

        # Get the name of the command.
        #
        # @example Get the command name.
        #   upconverter.command_name
        #
        # @return [ String, Symbol ] The command name.
        #
        # @since 2.4.0
        def command_name
          return FIND if filter[:$query]
          filter.keys.first
        end
      end
    end
  end
end

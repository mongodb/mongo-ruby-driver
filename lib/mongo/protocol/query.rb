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

    # MongoDB Wire protocol Query message.
    #
    # This is a client request message that is sent to the server in order
    # to retrieve documents matching provided query.
    #
    # Users may also provide additional options such as a projection, to
    # select a subset of the fields, a number to skip or a limit on the
    # number of returned documents.
    #
    # There are a variety of flags that can be used to adjust cursor
    # parameters or the desired consistancy and integrity the results.
    #
    # @api semipublic
    class Query < Message

      # Creates a new Query message
      #
      # @example Find all users named Tyler.
      #   Query.new('xgen', 'users', {:name => 'Tyler'})
      #
      # @example Find all users named Tyler skipping 5 and returning 10.
      #   Query.new('xgen', 'users', {:name => 'Tyler'}, :skip => 5,
      #                                                  :limit => 10)
      #
      # @example Find all users with slave ok bit set
      #   Query.new('xgen', 'users', {:name => 'Tyler'}, :flags => [:slave_ok])
      #
      # @example Find all user ids.
      #   Query.new('xgen', 'users', {}, :fields => {:id => 1})
      #
      # @param database [String, Symbol] The database to query.
      # @param collection [String, Symbol] The collection to query.
      # @param selector [Hash] The query selector.
      # @param options [Hash] The additional query options.
      #
      # @option options :project [Hash] The projection.
      # @option options :skip [Integer] The number of documents to skip.
      # @option options :limit [Integer] The number of documents to return.
      # @option options :flags [Array] The flags for the query message.
      #
      #   Supported flags: +:tailable_cursor+, +:slave_ok+, +:oplog_replay+,
      #   +:no_cursor_timeout+, +:await_data+, +:exhaust+, +:partial+
      def initialize(database, collection, selector, options = {})
        @database = database
        @namespace = "#{database}.#{collection}"
        @selector = selector
        @options = options
        @project = options[:project]
        @limit = determine_limit
        @skip = options[:skip]  || 0
        @flags = options[:flags] || []
        @upconverter = Upconverter.new(collection, selector, options, flags)
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
          command_name: upconverter.command_name,
          database_name: @database,
          command: upconverter.command,
          request_id: request_id
        }
      end

      # Query messages require replies from the database.
      #
      # @example Does the message require a reply?
      #   message.replyable?
      #
      # @return [ true ] Always true for queries.
      #
      # @since 2.0.0
      def replyable?
        true
      end

      private

      attr_reader :upconverter

      # The operation code required to specify a Query message.
      # @return [Fixnum] the operation code.
      def op_code
        2004
      end

      def determine_limit
        [ @options[:limit] || @options[:batch_size], @options[:batch_size] || @options[:limit] ].min || 0
      end

      # Available flags for a Query message.
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
      # @return [Array<Symbol>] The flags for this query message.
      field :flags, BitVector.new(FLAGS)

      # @!attribute
      # @return [String] The namespace for this query message.
      field :namespace, CString

      # @!attribute
      # @return [Integer] The number of documents to skip.
      field :skip, Int32

      # @!attribute
      # @return [Integer] The number of documents to return.
      field :limit, Int32

      # @!attribute
      # @return [Hash] The query selector.
      field :selector, Document

      # @!attribute
      # @return [Hash] The projection.
      field :project, Document

      # Converts legacy query messages to the appropriare OP_COMMAND style
      # message.
      #
      # @since 2.1.0
      class Upconverter

        # Mappings of the options to the find command options.
        #
        # @since 2.1.0
        OPTION_MAPPINGS = {
          :project => 'projection',
          :skip => 'skip',
          :limit => 'limit',
          :batch_size => 'batchSize'
        }.freeze

        SPECIAL_FIELD_MAPPINGS = {
          :$readPreference => 'readPreference',
          :$orderby => 'sort',
          :$hint => 'hint',
          :$comment => 'comment',
          :$returnKey => 'returnKey',
          :$snapshot => 'snapshot',
          :$maxScan => 'maxScan',
          :$max => 'max',
          :$min => 'min',
          :$maxTimeMS => 'maxTimeMS',
          :$showDiskLoc => 'showRecordId',
          :$explain => 'explain'
        }.freeze

        # Mapping of flags to find command options.
        #
        # @since 2.1.0
        FLAG_MAPPINGS = {
          :tailable_cursor => 'tailable',
          :oplog_replay => 'oplogReplay',
          :no_cursor_timeout => 'noCursorTimeout',
          :await_data => 'awaitData',
          :partial => 'allowPartialResults'
        }.freeze

        # Find command constant.
        #
        # @since 2.1.0
        FIND = 'find'.freeze

        # Filter attribute constant.
        #
        # @since 2.1.0
        FILTER = 'filter'.freeze

        # @return [ String ] collection The name of the collection.
        attr_reader :collection

        # @return [ BSON::Document, Hash ] filter The query filter or command.
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
        # @param [ BSON::Document, Hash ] filter The filter or command.
        # @param [ BSON::Document, Hash ] options The options.
        # @param [ Array<Symbol> ] flags The flags.
        #
        # @since 2.1.0
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
        # @since 2.1.0
        def command
          command? ? op_command : find_command
        end

        # Get the name of the command. If the collection is $cmd then it's the
        # first key in the filter, otherwise it's a find.
        #
        # @example Get the command name.
        #   upconverter.command_name
        #
        # @return [ String, Symbol ] The command name.
        #
        # @since 2.1.0
        def command_name
          command? ? filter.keys.first : FIND
        end

        private

        def command?
          collection == Database::COMMAND
        end

        def op_command
          document = BSON::Document.new
          filter.each do |field, value|
            document.store(field.to_s, value)
          end
          document
        end

        def find_command
          document = BSON::Document.new
          document.store(FIND, collection)
          document.store(FILTER, filter[:$query] ? filter[:$query] : filter)
          OPTION_MAPPINGS.each do |legacy, option|
            document.store(option, options[legacy]) unless options[legacy].nil?
          end
          SPECIAL_FIELD_MAPPINGS.each do |special, normal|
            document.store(normal, filter[special]) unless filter[special].nil?
          end
          FLAG_MAPPINGS.each do |legacy, flag|
            document.store(flag, true) if flags.include?(legacy)
          end
          document
        end
      end
    end
  end
end

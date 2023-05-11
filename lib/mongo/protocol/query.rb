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
    # parameters or the desired consistency and integrity the results.
    #
    # @api semipublic
    class Query < Message
      include Monitoring::Event::Secure

      # Creates a new Query message
      #
      # @example Find all users named Tyler.
      #   Query.new('xgen', 'users', {:name => 'Tyler'})
      #
      # @example Find all users named Tyler skipping 5 and returning 10.
      #   Query.new('xgen', 'users', {:name => 'Tyler'}, :skip => 5,
      #                                                  :limit => 10)
      #
      # @example Find all users with secondaryOk bit set
      #   Query.new('xgen', 'users', {:name => 'Tyler'}, :flags => [:secondary_ok])
      #
      # @example Find all user ids.
      #   Query.new('xgen', 'users', {}, :fields => {:id => 1})
      #
      # @param [ String, Symbol ] database The database to query.
      # @param [ String, Symbol ] collection The collection to query.
      # @param [ Hash ] selector The query selector.
      # @param [ Hash ] options The additional query options.
      #
      # @option options [ Array<Symbol> ] :flags The flag bits.
      #   Currently supported values are :await_data, :exhaust,
      #   :no_cursor_timeout, :oplog_replay, :partial, :secondary_ok,
      #   :tailable_cursor.
      # @option options [ Integer ] :limit The number of documents to return.
      # @option options [ Hash ] :project The projection.
      # @option options [ Integer ] :skip The number of documents to skip.
      def initialize(database, collection, selector, options = {})
        @database = database
        @namespace = "#{database}.#{collection}"
        if selector.nil?
          raise ArgumentError, 'Selector cannot be nil'
        end
        @selector = selector
        @options = options
        @project = options[:project]
        @limit = determine_limit
        @skip = options[:skip]  || 0
        @flags = options[:flags] || []
        @upconverter = Upconverter.new(
          collection,
          BSON::Document.new(selector),
          BSON::Document.new(options),
          flags,
        )
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
          command_name: upconverter.command_name,
          database_name: @database,
          command: upconverter.command,
          request_id: request_id
        )
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

      # Compress the message, if the command being sent permits compression.
      # Otherwise returns self.
      #
      # @param [ String, Symbol ] compressor The compressor to use.
      # @param [ Integer ] zlib_compression_level The zlib compression level to use.
      #
      # @return [ Message ] A Protocol::Compressed message or self,
      #  depending on whether this message can be compressed.
      #
      # @since 2.5.0
      # @api private
      def maybe_compress(compressor, zlib_compression_level = nil)
        compress_if_possible(selector.keys.first, compressor, zlib_compression_level)
      end

      # Serializes message into bytes that can be sent on the wire.
      #
      # @param [ BSON::ByteBuffer ] buffer where the message should be inserted.
      # @param [ Integer ] max_bson_size The maximum bson object size.
      #
      # @return [ BSON::ByteBuffer ] buffer containing the serialized message.
      def serialize(buffer = BSON::ByteBuffer.new, max_bson_size = nil, bson_overhead = nil)
        validate_document_size!(max_bson_size)

        super
      end

      protected

      attr_reader :upconverter

      private

      # Validate that the documents in this message are all smaller than the
      # maxBsonObjectSize. If not, raise an exception.
      def validate_document_size!(max_bson_size)
        max_bson_size ||= Mongo::Server::ConnectionBase::DEFAULT_MAX_BSON_OBJECT_SIZE

        documents = if @selector.key?(:documents)
                      @selector[:documents]
                    elsif @selector.key?(:deletes)
                      @selector[:deletes]
                    elsif @selector.key?(:updates)
                      @selector[:updates]
                    else
                      []
                    end

        contains_too_large_document = documents.any? do |doc|
          doc.to_bson.length > max_bson_size
        end

        if contains_too_large_document
          raise Error::MaxBSONSize.new('The document exceeds maximum allowed BSON object size after serialization')
        end
      end

      # The operation code required to specify a Query message.
      # @return [Fixnum] the operation code.
      #
      # @since 2.5.0
      OP_CODE = 2004

      def determine_limit
        [ @options[:limit] || @options[:batch_size], @options[:batch_size] || @options[:limit] ].min || 0
      end

      # Available flags for a Query message.
      # @api private
      FLAGS = [
        :reserved,
        :tailable_cursor,
        :secondary_ok,
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
          :$readPreference => '$readPreference',
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
          # Although the docstring claims both hashes and BSON::Documents
          # are acceptable, this class expects the filter and options to
          # contain symbol keys which isn't what the operation layer produces.
          unless BSON::Document === filter
            raise ArgumentError, 'Filter must provide indifferent access'
          end
          unless BSON::Document === options
            raise ArgumentError, 'Options must provide indifferent access'
          end
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
        # @return [ String ] The command name.
        #
        # @since 2.1.0
        def command_name
          ((filter[:$query] || !command?) ? :find : filter.keys.first).to_s
        end

        private

        def command?
          collection == Database::COMMAND
        end

        def query_filter
          filter[:$query] || filter
        end

        def op_command
          document = BSON::Document.new
          query_filter.each do |field, value|
            document.store(field.to_s, value)
          end
          document
        end

        def find_command
          document = BSON::Document.new(
            find: collection,
            filter: query_filter,
          )
          OPTION_MAPPINGS.each do |legacy, option|
            document.store(option, options[legacy]) unless options[legacy].nil?
          end
          if Lint.enabled?
            filter.each do |k, v|
              unless String === k
                raise Error::LintError, "All keys in filter must be strings: #{filter.inspect}"
              end
            end
          end
          Lint.validate_camel_case_read_preference(filter['readPreference'])
          SPECIAL_FIELD_MAPPINGS.each do |special, normal|
            unless (v = filter[special]).nil?
              document.store(normal, v)
            end
          end
          FLAG_MAPPINGS.each do |legacy, flag|
            document.store(flag, true) if flags.include?(legacy)
          end
          document
        end
      end

      Registry.register(OP_CODE, self)
    end
  end
end

# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2017-2020 MongoDB Inc.
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

    # MongoDB Wire protocol Msg message (OP_MSG), a bi-directional wire
    # protocol opcode.
    #
    # OP_MSG is only available in MongoDB 3.6 (maxWireVersion >= 6) and later.
    #
    # @api private
    #
    # @since 2.5.0
    class Msg < Message
      include Monitoring::Event::Secure

      # The identifier for the database name to execute the command on.
      #
      # @since 2.5.0
      DATABASE_IDENTIFIER = '$db'.freeze

      # Keys that the driver adds to commands. These are going to be
      # moved to the end of the hash for better logging.
      #
      # @api private
      INTERNAL_KEYS = Set.new(%w($clusterTime $db lsid signature txnNumber)).freeze

      # Creates a new OP_MSG protocol message
      #
      # @example Create a OP_MSG wire protocol message
      #   Msg.new([:more_to_come], {}, { hello: 1 },
      #           { type: 1, payload: { identifier: 'documents', sequence: [..] } })
      #
      # @param [ Array<Symbol> ] flags The flag bits. Currently supported
      #   values are :more_to_come and :checksum_present.
      # @param [ Hash ] options The options.
      # @param [ BSON::Document, Hash ] main_document The document that will
      #   become the payload type 0 section. Can contain global args as they
      #   are defined in the OP_MSG specification.
      # @param [ Protocol::Msg::Section1 ] sequences Zero or more payload type 1
      #   sections.
      #
      # @option options [ true, false ] validating_keys Whether keys should be
      #   validated for being valid document keys (i.e. not begin with $ and
      #   not contain dots).
      #   This option is deprecated and will not be used. It will removed in version 3.0.
      #
      # @api private
      #
      # @since 2.5.0
      def initialize(flags, options, main_document, *sequences)
        if flags
          flags.each do |flag|
            unless KNOWN_FLAGS.key?(flag)
              raise ArgumentError, "Unknown flag: #{flag.inspect}"
            end
          end
        end
        @flags = flags || []
        @options = options
        unless main_document.is_a?(Hash)
          raise ArgumentError, "Main document must be a Hash, given: #{main_document.class}"
        end
        @main_document = main_document
        sequences.each_with_index do |section, index|
          unless section.is_a?(Section1)
            raise ArgumentError, "All sequences must be Section1 instances, got: #{section} at index #{index}"
          end
        end
        @sequences = sequences
        @sections = [
          {type: 0, payload: @main_document}
        ] + @sequences.map do |section|
          {type: 1, payload: {
            identifier: section.identifier,
            sequence: section.documents.map do |doc|
              CachingHash.new(doc)
            end,
          }}
        end
        @request_id = nil
        super
      end

      # Whether the message expects a reply from the database.
      #
      # @example Does the message require a reply?
      #   message.replyable?
      #
      # @return [ true, false ] If the message expects a reply.
      #
      # @since 2.5.0
      def replyable?
        @replyable ||= !flags.include?(:more_to_come)
      end

      # Return the event payload for monitoring.
      #
      # @example Return the event payload.
      #   message.payload
      #
      # @return [ BSON::Document ] The event payload.
      #
      # @since 2.5.0
      def payload
        # Reorder keys in main_document for better logging - see
        # https://jira.mongodb.org/browse/RUBY-1591.
        # Note that even without the reordering, the payload is not an exact
        # match to what is sent over the wire because the command as used in
        # the published event combines keys from multiple sections of the
        # payload sent over the wire.
        ordered_command = {}
        skipped_command = {}
        command.each do |k, v|
          if INTERNAL_KEYS.member?(k.to_s)
            skipped_command[k] = v
          else
            ordered_command[k] = v
          end
        end
        ordered_command.update(skipped_command)

        BSON::Document.new(
          command_name: ordered_command.keys.first.to_s,
          database_name: @main_document[DATABASE_IDENTIFIER],
          command: ordered_command,
          request_id: request_id,
          reply: @main_document,
        )
      end

      # Serializes message into bytes that can be sent on the wire.
      #
      # @param [ BSON::ByteBuffer ] buffer where the message should be inserted.
      # @param [ Integer ] max_bson_size The maximum bson object size.
      #
      # @return [ BSON::ByteBuffer ] buffer containing the serialized message.
      #
      # @since 2.5.0
      def serialize(buffer = BSON::ByteBuffer.new, max_bson_size = nil, bson_overhead = nil)
        validate_document_size!(max_bson_size)

        super
        add_check_sum(buffer)
        buffer
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
        compress_if_possible(command.keys.first, compressor, zlib_compression_level)
      end

      # Reverse-populates the instance variables after deserialization sets
      # the @sections instance variable to the list of documents.
      #
      # TODO fix deserialization so that this method is not needed.
      #
      # @api private
      def fix_after_deserialization
        if @sections.nil?
          raise NotImplementedError, "After deserializations @sections should have been initialized"
        end
        if @sections.length != 1
          raise NotImplementedError, "Deserialization must have produced exactly one section, but it produced #{sections.length} sections"
        end
        @main_document = @sections.first
        @sequences = []
        @sections = [{type: 0, payload: @main_document}]
      end

      def documents
        [@main_document]
      end

      # Possibly encrypt this message with libmongocrypt. Message will only be
      # encrypted if the specified client exists, that client has been given
      # auto-encryption options, the client has not been instructed to bypass
      # auto-encryption, and mongocryptd determines that this message is
      # eligible for encryption. A message is eligible for encryption if it
      # represents one of the command types allow-listed by libmongocrypt and it
      # contains data that is required to be encrypted by a local or remote json schema.
      #
      # @param [ Mongo::Server::Connection ] connection The connection on which
      #   the operation is performed.
      # @param [ Mongo::Operation::Context ] context The operation context.
      #
      # @return [ Mongo::Protocol::Msg ] The encrypted message, or the original
      #   message if encryption was not possible or necessary.
      def maybe_encrypt(connection, context)
        # TODO verify compression happens later, i.e. when this method runs
        # the message is not compressed.
        if context.encrypt?
          if connection.description.max_wire_version < 8
            raise Error::CryptError.new(
              "Cannot perform encryption against a MongoDB server older than " +
              "4.2 (wire version less than 8). Currently connected to server " +
              "with max wire version #{connection.description.max_wire_version}} " +
              "(Auto-encryption requires a minimum MongoDB version of 4.2)"
            )
          end

          db_name = @main_document[DATABASE_IDENTIFIER]
          cmd = merge_sections
          enc_cmd = context.encrypter.encrypt(db_name, cmd)
          if cmd.key?('$db') && !enc_cmd.key?('$db')
            enc_cmd['$db'] = cmd['$db']
          end

          Msg.new(@flags, @options, enc_cmd)
        else
          self
        end
      end

      # Possibly decrypt this message with libmongocrypt. Message will only be
      # decrypted if the specified client exists, that client has been given
      # auto-encryption options, and this message is eligible for decryption.
      # A message is eligible for decryption if it represents one of the command
      # types allow-listed by libmongocrypt and it contains data that is required
      # to be encrypted by a local or remote json schema.
      #
      # @param [ Mongo::Operation::Context ] context The operation context.
      #
      # @return [ Mongo::Protocol::Msg ] The decrypted message, or the original
      #   message if decryption was not possible or necessary.
      def maybe_decrypt(context)
        if context.decrypt?
          cmd = merge_sections
          enc_cmd = context.encrypter.decrypt(cmd)
          Msg.new(@flags, @options, enc_cmd)
        else
          self
        end
      end

      # Whether this message represents a bulk write. A bulk write is an insert,
      # update, or delete operation that encompasses multiple operations of
      # the same type.
      #
      # @return [ Boolean ] Whether this message represents a bulk write.
      #
      # @note This method was written to support client-side encryption
      #   functionality. It is not recommended that this method be used in
      #   service of any other feature or behavior.
      #
      # @api private
      def bulk_write?
        inserts = @main_document['documents']
        updates = @main_document['updates']
        deletes = @main_document['deletes']

        num_inserts = inserts && inserts.length || 0
        num_updates = updates && updates.length || 0
        num_deletes = deletes && deletes.length || 0

        num_inserts > 1  || num_updates > 1 || num_deletes > 1
      end

      def maybe_add_server_api(server_api)
        conflicts = {}
        %i(apiVersion apiStrict apiDeprecationErrors).each do |key|
          if @main_document.key?(key)
            conflicts[key] = @main_document[key]
          end
          if @main_document.key?(key.to_s)
            conflicts[key] = @main_document[key.to_s]
          end
        end
        unless conflicts.empty?
          raise Error::ServerApiConflict, "The Client is configured with :server_api option but the operation provided the following conflicting parameters: #{conflicts.inspect}"
        end

        main_document = @main_document.merge(
          Utils.transform_server_api(server_api)
        )
        Msg.new(@flags, @options, main_document, *@sequences)
      end

      # Returns the number of documents returned from the server.
      #
      # The Msg instance must be for a server reply and the reply must return
      # an active cursor (either a newly created one or one whose iteration is
      # continuing via getMore).
      #
      # @return [ Integer ] Number of returned documents.
      def number_returned
        if doc = documents.first
          if cursor = doc['cursor']
            if batch = cursor['firstBatch'] || cursor['nextBatch']
              return batch.length
            end
          end
        end
        raise NotImplementedError, "number_returned is only defined for cursor replies"
      end

      private

      # Validate that the documents in this message are all smaller than the
      # maxBsonObjectSize. If not, raise an exception.
      def validate_document_size!(max_bson_size)
        max_bson_size ||= Mongo::Server::ConnectionBase::DEFAULT_MAX_BSON_OBJECT_SIZE

        contains_too_large_document = @sections.any? do |section|
          section[:type] == 1 &&
            section[:payload][:sequence].any? do |document|
              document.to_bson.length > max_bson_size
            end
        end

        if contains_too_large_document
          raise Error::MaxBSONSize.new('The document exceeds maximum allowed BSON object size after serialization')
        end
      end

      def command
        @command ||= if @main_document
          @main_document.dup.tap do |cmd|
            @sequences.each do |section|
              cmd[section.identifier] ||= []
              cmd[section.identifier] += section.documents
            end
          end
        else
          documents.first
        end
      end

      def add_check_sum(buffer)
        if flags.include?(:checksum_present)
          #buffer.put_int32(checksum)
        end
      end

      # Encapsulates a type 1 OP_MSG section.
      #
      # @see https://github.com/mongodb/specifications/blob/master/source/message/OP_MSG.rst#sections
      #
      # @api private
      class Section1
        def initialize(identifier, documents)
          @identifier, @documents = identifier, documents
        end

        attr_reader :identifier, :documents

        def ==(other)
          other.is_a?(Section1) &&
            identifier == other.identifier && documents == other.documents
        end

        alias :eql? :==
      end

      # The operation code required to specify a OP_MSG message.
      # @return [ Fixnum ] the operation code.
      #
      # @since 2.5.0
      OP_CODE = 2013

      KNOWN_FLAGS = {
        checksum_present: true,
        more_to_come: true,
        exhaust_allowed: true,
      }

      # Available flags for a OP_MSG message.
      FLAGS = Array.new(16).tap do |arr|
        arr[0] = :checksum_present
        arr[1] = :more_to_come
        arr[16] = :exhaust_allowed
      end.freeze

      # @!attribute
      # @return [Array<Symbol>] The flags for this message.
      field :flags, BitVector.new(FLAGS)

      # The sections that will be serialized, or the documents have been
      # deserialized.
      #
      # Usually the sections contain OP_MSG-compliant sections derived
      # from @main_document and @sequences. The information in @main_document
      # and @sequences is duplicated in the sections.
      #
      # When deserializing Msg instances, sections temporarily is an array
      # of documents returned in the type 0 section of the OP_MSG wire
      # protocol message. #fix_after_deserialization method mutates this
      # object to have sections, @main_document and @sequences be what
      # they would have been had the Msg instance been constructed using
      # the constructor (rather than having been deserialized).
      #
      # @return [ Array<Hash> | Array<BSON::Document> ] The sections of
      #   payload type 1 or 0.
      # @api private
      field :sections, Sections

      Registry.register(OP_CODE, self)
    end
  end
end

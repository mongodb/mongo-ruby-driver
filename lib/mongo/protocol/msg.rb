# Copyright (C) 2017-2019 MongoDB, Inc.
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
      INTERNAL_KEYS = Set.new(%w($clusterTime lsid signature txnNumber)).freeze

      # Creates a new OP_MSG protocol message
      #
      # @example Create a OP_MSG wire protocol message
      #   Msg.new([:more_to_come], {}, { ismaster: 1 },
      #           { type: 1, payload: { identifier: 'documents', sequence: [..] } })
      #
      # @param [ Array<Symbol> ] flags The flag bits. Current supported values
      # are :more_to_come and :checksum_present.
      # @param [ Hash ] options The options. There are currently no supported
      #   options, this is a placeholder for the future.
      # @param [ BSON::Document, Hash ] global_args The global arguments,
      #   becomes a section of payload type 0.
      # @param [ BSON::Document, Hash ] sections Zero or more sections, in the format
      #   { type: 1, payload: { identifier: <String>, sequence: <Array<BSON::Document, Hash>> } } or
      #   { type: 0, payload: <BSON::Document, Hash> }
      #
      # @option options [ true, false ] validating_keys Whether keys should be validated.
      #
      # @api private
      #
      # @since 2.5.0
      def initialize(flags, options, global_args, *sections)
        @flags = flags || []
        @options = options
        @global_args = global_args
        @sections = [ { type: 0, payload: global_args } ] + sections
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
        # Reorder keys in global_args for better logging - see
        # https://jira.mongodb.org/browse/RUBY-1591.
        # Note that even without the reordering, the payload is not an exact
        # match to what is sent over the wire because the command as used in
        # the published eent combines keys from multiple sections of the
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
          database_name: global_args[DATABASE_IDENTIFIER],
          command: ordered_command,
          request_id: request_id,
          reply: sections[0]
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
      def serialize(buffer = BSON::ByteBuffer.new, max_bson_size = nil)
        super
        add_check_sum(buffer)
        buffer
      end

      # Compress this message.
      #
      # @param [ String, Symbol ] compressor The compressor to use.
      # @param [ Integer ] zlib_compression_level The zlib compression level to use.
      #
      # @return [ Compressed, self ] A Protocol::Compressed message or self, depending on whether
      #  this message can be compressed.
      #
      # @since 2.5.0
      def compress!(compressor, zlib_compression_level = nil)
        if compressor && compression_allowed?(command.keys.first)
          Compressed.new(self, compressor, zlib_compression_level)
        else
          self
        end
      end

      private

      def command
        @command ||= global_args.dup.tap do |cmd|
          cmd.delete(DATABASE_IDENTIFIER)
          sections.each do |section|
            if section[:type] == 1
              identifier = section[:payload][:identifier]
              cmd[identifier] ||= []
              cmd[identifier] += section[:payload][:sequence]
            end
          end
        end
      end

      def add_check_sum(buffer)
        if flags.include?(:checksum_present)
          #buffer.put_int32(checksum)
        end
      end

      def global_args
        @global_args ||= (sections[0] || {})
      end

      # The operation code required to specify a OP_MSG message.
      # @return [ Fixnum ] the operation code.
      #
      # @since 2.5.0
      OP_CODE = 2013

      # Available flags for a OP_MSG message.
      FLAGS = Array.new(16).tap { |arr|
        arr[0] = :checksum_present
        arr[1] = :more_to_come
      }

      # @!attribute
      # @return [Array<Symbol>] The flags for this message.
      field :flags, BitVector.new(FLAGS)

      # @!attribute
      # @return [Hash] The sections of payload type 1 or 0.
      field :sections, Sections
      alias :documents :sections

      Registry.register(OP_CODE, self)
    end
  end
end

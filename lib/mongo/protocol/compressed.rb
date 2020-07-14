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

    # MongoDB Wire protocol Compressed message.
    #
    # This is a bi-directional message that compresses another opcode.
    #
    # @api semipublic
    #
    # @since 2.5.0
    class Compressed < Message

      # The byte signaling that the message has been compressed with Zlib.
      #
      # @since 2.5.0
      ZLIB_BYTE = 2.chr.force_encoding(BSON::BINARY).freeze

      # The Zlib compressor identifier.
      #
      # @since 2.5.0
      ZLIB = 'zlib'.freeze

      # The compressor identifier to byte map.
      #
      # @since 2.5.0
      COMPRESSOR_ID_MAP = { ZLIB => ZLIB_BYTE }.freeze

      # Creates a new OP_COMPRESSED message.
      #
      # @example Create an OP_COMPRESSED message.
      #   Compressed.new(original_message, 'zlib')
      #
      # @param [ Mongo::Protocol::Message ] message The original message.
      # @param [ String, Symbol ] compressor The compression algorithm to use.
      # @param [ Integer ] zlib_compression_level The zlib compression level to use.
      #   -1 and nil imply default.
      #
      # @since 2.5.0
      def initialize(message, compressor, zlib_compression_level = nil)
        @original_message = message
        @original_op_code = message.op_code
        @uncompressed_size = 0
        @compressor_id = COMPRESSOR_ID_MAP[compressor]
        @compressed_message = ''
        @zlib_compression_level = zlib_compression_level if zlib_compression_level && zlib_compression_level != -1
        @request_id = message.request_id
      end

      # Inflates an OP_COMRESSED message and returns the original message.
      #
      # @return [ Protocol::Message ] The inflated message.
      #
      # @since 2.5.0
      # @api private
      def maybe_inflate
        message = Registry.get(@original_op_code).allocate
        uncompressed_message = Zlib::Inflate.inflate(@compressed_message)

        buf = BSON::ByteBuffer.new(uncompressed_message)

        message.send(:fields).each do |field|
          if field[:multi]
            Message.deserialize_array(message, buf, field)
          else
            Message.deserialize_field(message, buf, field)
          end
        end
        if message.is_a?(Msg)
          message.fix_after_deserialization
        end
        message
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
        @original_message.replyable?
      end

      private

      # The operation code for a +Compressed+ message.
      # @return [ Fixnum ] the operation code.
      #
      # @since 2.5.0
      OP_CODE = 2012

      # @!attribute
      # Field representing the original message's op code as an Int32.
      field :original_op_code, Int32

      # @!attribute
      # @return [ Fixnum ] The size of the original message, excluding header as an Int32.
      field :uncompressed_size, Int32

      # @!attribute
      # @return [ String ] The id of the compressor as a single byte.
      field :compressor_id, Byte

      # @!attribute
      # @return [ String ] The actual compressed message bytes.
      field :compressed_message, Bytes

      def serialize_fields(buffer, max_bson_size)
        buf = BSON::ByteBuffer.new
        @original_message.send(:serialize_fields, buf, max_bson_size)
        @uncompressed_size = buf.length
        @compressed_message = Zlib::Deflate.deflate(buf.to_s, @zlib_compression_level).force_encoding(BSON::BINARY)
        super
      end

      Registry.register(OP_CODE, self)
    end
  end
end

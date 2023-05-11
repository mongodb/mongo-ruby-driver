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

    # Container for various serialization strategies
    #
    # Each strategy must have a serialization method named +serailize+
    # and a deserialization method named +deserialize+
    #
    # Serialize methods must take buffer and value arguements and
    # serialize the value into the buffer
    #
    # Deserialize methods must take an IO stream argument and
    # deserialize the value from the stream of bytes
    #
    # @api private
    module Serializers

      private

      ZERO = 0.freeze
      NULL = 0.chr.freeze
      INT32_PACK = 'l<'.freeze
      INT64_PACK = 'q<'.freeze
      HEADER_PACK = 'l<l<l<l<'.freeze

      # MongoDB wire protocol serialization strategy for message headers.
      #
      # Serializes and de-serializes four 32-bit integers consisting
      # of the length of the message, the request id, the response id,
      # and the op code for the operation.
      module Header

        # Serializes the header value into the buffer
        #
        # @param buffer [ String ] Buffer to receive the serialized value.
        # @param value [ String ] Header value to be serialized.
        # @param [ true, false ] validating_keys Whether keys should be validated when serializing.
        #   This option is deprecated and will not be used. It will removed in version 3.0.
        #
        # @return [ String ] Buffer with serialized value.
        def self.serialize(buffer, value, validating_keys = nil)
          buffer.put_bytes(value.pack(HEADER_PACK))
        end

        # Deserializes the header value from the IO stream
        #
        # @param [ String ] buffer Buffer containing the message header.
        # @param [ Hash ] options This method currently accepts no options.
        #
        # @return [ Array<Fixnum> ] Array consisting of the deserialized
        #   length, request id, response id, and op code.
        def self.deserialize(buffer, options = {})
          buffer.get_bytes(16).unpack(HEADER_PACK)
        end
      end

      # MongoDB wire protocol serialization strategy for C style strings.
      #
      # Serializes and de-serializes C style strings (null terminated).
      module CString

        # Serializes a C style string into the buffer
        #
        # @param buffer [ String ] Buffer to receive the serialized CString.
        # @param value [ String ] The string to be serialized.
        #
        # @return [ String ] Buffer with serialized value.
        def self.serialize(buffer, value, validating_keys = nil)
          buffer.put_cstring(value)
        end
      end

      # MongoDB wire protocol serialization strategy for 32-bit Zero.
      #
      # Serializes and de-serializes one 32-bit Zero.
      module Zero

        # Serializes a 32-bit Zero into the buffer
        #
        # @param buffer [ String ] Buffer to receive the serialized Zero.
        # @param value [ Fixnum ] Ignored value.
        #
        # @return [ String ] Buffer with serialized value.
        def self.serialize(buffer, value, validating_keys = nil)
          buffer.put_int32(ZERO)
        end
      end

      # MongoDB wire protocol serialization strategy for 32-bit integers.
      #
      # Serializes and de-serializes one 32-bit integer.
      module Int32

        # Serializes a number to a 32-bit integer
        #
        # @param buffer [ String ] Buffer to receive the serialized Int32.
        # @param value [ Integer | BSON::Int32 ] 32-bit integer to be serialized.
        #
        # @return [String] Buffer with serialized value.
        def self.serialize(buffer, value, validating_keys = nil)
          if value.is_a?(BSON::Int32)
            if value.respond_to?(:value)
              # bson-ruby >= 4.6.0
              value = value.value
            else
              value = value.instance_variable_get('@integer')
            end
          end
          buffer.put_int32(value)
        end

        # Deserializes a 32-bit Fixnum from the IO stream
        #
        # @param [ String ] buffer Buffer containing the 32-bit integer
        # @param [ Hash ] options This method currently accepts no options.
        #
        # @return [ Fixnum ] Deserialized Int32
        def self.deserialize(buffer, options = {})
          buffer.get_int32
        end
      end

      # MongoDB wire protocol serialization strategy for 64-bit integers.
      #
      # Serializes and de-serializes one 64-bit integer.
      module Int64

        # Serializes a number to a 64-bit integer
        #
        # @param buffer [ String ] Buffer to receive the serialized Int64.
        # @param value [ Integer | BSON::Int64 ] 64-bit integer to be serialized.
        #
        # @return [ String ] Buffer with serialized value.
        def self.serialize(buffer, value, validating_keys = nil)
          if value.is_a?(BSON::Int64)
            if value.respond_to?(:value)
              # bson-ruby >= 4.6.0
              value = value.value
            else
              value = value.instance_variable_get('@integer')
            end
          end
          buffer.put_int64(value)
        end

        # Deserializes a 64-bit Fixnum from the IO stream
        #
        # @param [ String ] buffer Buffer containing the 64-bit integer.
        # @param [ Hash ] options This method currently accepts no options.
        #
        # @return [Fixnum] Deserialized Int64.
        def self.deserialize(buffer, options = {})
          buffer.get_int64
        end
      end

      # MongoDB wire protocol serialization strategy for a Section of OP_MSG.
      #
      # Serializes and de-serializes a list of Sections.
      #
      # @since 2.5.0
      module Sections

        # Serializes the sections of an OP_MSG, payload type 0 or 1.
        #
        # @param [ BSON::ByteBuffer ] buffer Buffer to receive the serialized Sections.
        # @param [ Array<Hash, BSON::Document> ] value The sections to be serialized.
        # @param [ Fixnum ] max_bson_size The max bson size of documents in the sections.
        # @param [ true, false ] validating_keys Whether to validate document keys.
        #   This option is deprecated and will not be used. It will removed in version 3.0.
        #
        # @return [ BSON::ByteBuffer ] Buffer with serialized value.
        #
        # @since 2.5.0
        def self.serialize(buffer, value, max_bson_size = nil, validating_keys = nil)
          value.each do |section|
            case section[:type]
            when PayloadZero::TYPE
              PayloadZero.serialize(buffer, section[:payload], max_bson_size)
            when nil
              PayloadZero.serialize(buffer, section[:payload], max_bson_size)
            when PayloadOne::TYPE
              PayloadOne.serialize(buffer, section[:payload], max_bson_size)
            else
              raise Error::UnknownPayloadType.new(section[:type])
            end
          end
        end

        # Deserializes a section of an OP_MSG from the IO stream.
        #
        # @param [ BSON::ByteBuffer ] buffer Buffer containing the sections.
        # @param [ Hash ] options
        #
        # @option options [ Boolean ] :deserialize_as_bson Whether to perform
        #   section deserialization using BSON types instead of native Ruby types
        #   wherever possible.
        #
        # @return [ Array<BSON::Document> ] Deserialized sections.
        #
        # @since 2.5.0
        def self.deserialize(buffer, options = {})
          end_length = (@flag_bits & Msg::FLAGS.index(:checksum_present)) == 1 ? 32 : 0
          sections = []
          until buffer.length == end_length
            case byte = buffer.get_byte
            when PayloadZero::TYPE_BYTE
              sections << PayloadZero.deserialize(buffer, options)
            when PayloadOne::TYPE_BYTE
              sections += PayloadOne.deserialize(buffer, options)
            else
              raise Error::UnknownPayloadType.new(byte)
            end
          end
          sections
        end

        # Whether there can be a size limit on this type after serialization.
        #
        # @return [ true ] Documents can be size limited upon serialization.
        #
        # @since 2.5.0
        def self.size_limited?
          true
        end

        # MongoDB wire protocol serialization strategy for a payload 0 type Section of OP_MSG.
        #
        # @since 2.5.0
        module PayloadZero

          # The byte identifier for this payload type.
          #
          # @since 2.5.0
          TYPE = 0x0

          # The byte corresponding to this payload type.
          #
          # @since 2.5.0
          TYPE_BYTE = TYPE.chr.force_encoding(BSON::BINARY).freeze

          # Serializes a section of an OP_MSG, payload type 0.
          #
          # @param [ BSON::ByteBuffer ] buffer Buffer to receive the serialized Sections.
          # @param [ BSON::Document, Hash ] value The object to serialize.
          # @param [ Fixnum ] max_bson_size The max bson size of documents in the section.
          # @param [ true, false ] validating_keys Whether to validate document keys.
          #   This option is deprecated and will not be used. It will removed in version 3.0.
          #
          # @return [ BSON::ByteBuffer ] Buffer with serialized value.
          #
          # @since 2.5.0
          def self.serialize(buffer, value, max_bson_size = nil, validating_keys = nil)
            buffer.put_byte(TYPE_BYTE)
            Serializers::Document.serialize(buffer, value, max_bson_size)
          end

          # Deserializes a section of payload type 0 of an OP_MSG from the IO stream.
          #
          # @param [ BSON::ByteBuffer ] buffer Buffer containing the sections.
          # @param [ Hash ] options
          #
          # @option options [ Boolean ] :deserialize_as_bson Whether to perform
          #   section deserialization using BSON types instead of native Ruby types
          #   wherever possible.
          #
          # @return [ Array<BSON::Document> ] Deserialized section.
          #
          # @since 2.5.0
          def self.deserialize(buffer, options = {})
            mode = options[:deserialize_as_bson] ? :bson : nil
            BSON::Document.from_bson(buffer, **{ mode: mode })
          end
        end

        # MongoDB wire protocol serialization strategy for a payload 1 type Section of OP_MSG.
        #
        # @since 2.5.0
        module PayloadOne

          # The byte identifier for this payload type.
          #
          # @since 2.5.0
          TYPE = 0x1

          # The byte corresponding to this payload type.
          #
          # @since 2.5.0
          TYPE_BYTE = TYPE.chr.force_encoding(BSON::BINARY).freeze

          # Serializes a section of an OP_MSG, payload type 1.
          #
          # @param [ BSON::ByteBuffer ] buffer Buffer to receive the serialized Sections.
          # @param [ BSON::Document, Hash ] value The object to serialize.
          # @param [ Fixnum ] max_bson_size The max bson size of documents in the section.
          # @param [ true, false ] validating_keys Whether to validate document keys.
          #   This option is deprecated and will not be used. It will removed in version 3.0.
          #
          # @return [ BSON::ByteBuffer ] Buffer with serialized value.
          #
          # @since 2.5.0
          def self.serialize(buffer, value, max_bson_size = nil, validating_keys = nil)
            buffer.put_byte(TYPE_BYTE)
            start = buffer.length
            buffer.put_int32(0) # hold for size
            buffer.put_cstring(value[:identifier])
            value[:sequence].each do |document|
              Document.serialize(buffer, document, max_bson_size)
            end
            buffer.replace_int32(start, buffer.length - start)
          end

          # Deserializes a section of payload type 1 of an OP_MSG from the IO stream.
          #
          # @param [ BSON::ByteBuffer ] buffer Buffer containing the sections.
          #
          # @return [ Array<BSON::Document> ] Deserialized section.
          #
          # @since 2.5.0
          def self.deserialize(buffer)
            raise NotImplementedError

            start_size = buffer.length
            section_size = buffer.get_int32 # get the size
            end_size = start_size - section_size
            buffer.get_cstring # get the identifier
            documents = []
            until buffer.length == end_size
              documents << BSON::Document.from_bson(buffer)
            end
            documents
          end
        end
      end

      # MongoDB wire protocol serialization strategy for a BSON Document.
      #
      # Serializes and de-serializes a single document.
      module Document

        # Serializes a document into the buffer
        #
        # @param buffer [ String ] Buffer to receive the BSON encoded document.
        # @param value [ Hash ] Document to serialize as BSON.
        #
        # @return [ String ] Buffer with serialized value.
        def self.serialize(buffer, value, max_bson_size = nil, validating_keys = nil)
          start_size = buffer.length
          value.to_bson(buffer)
          serialized_size = buffer.length - start_size
          if max_bson_size && serialized_size > max_bson_size
            raise Error::MaxBSONSize,
              "The document exceeds maximum allowed BSON object size after serialization. Serialized size: #{serialized_size} bytes, maximum allowed size: #{max_bson_size} bytes"
          end
        end

        # Deserializes a document from the IO stream
        #
        # @param [ String ] buffer Buffer containing the BSON encoded document.
        # @param [ Hash ] options
        #
        # @option options [ Boolean ] :deserialize_as_bson Whether to perform
        #   section deserialization using BSON types instead of native Ruby types
        #   wherever possible.
        #
        # @return [ Hash ] The decoded BSON document.
        def self.deserialize(buffer, options = {})
          mode = options[:deserialize_as_bson] ? :bson : nil
          BSON::Document.from_bson(buffer, **{ mode: mode })
        end

        # Whether there can be a size limit on this type after serialization.
        #
        # @return [ true ] Documents can be size limited upon serialization.
        #
        # @since 2.0.0
        def self.size_limited?
          true
        end
      end

      # MongoDB wire protocol serialization strategy for a single byte.
      #
      # Writes and fetches a single byte from the byte buffer.
      module Byte

        # Writes a byte into the buffer.
        #
        # @param [ BSON::ByteBuffer ] buffer Buffer to receive the single byte.
        # @param [ String ] value The byte to write to the buffer.
        # @param [ true, false ] validating_keys Whether to validate keys.
        #   This option is deprecated and will not be used. It will removed in version 3.0.
        #
        # @return [ BSON::ByteBuffer ] Buffer with serialized value.
        #
        # @since 2.5.0
        def self.serialize(buffer, value, validating_keys = nil)
          buffer.put_byte(value)
        end

        # Deserializes a byte from the byte buffer.
        #
        # @param [ BSON::ByteBuffer ] buffer Buffer containing the value to read.
        # @param [ Hash ] options This method currently accepts no options.
        #
        # @return [ String ] The byte.
        #
        # @since 2.5.0
        def self.deserialize(buffer, options = {})
          buffer.get_byte
        end
      end

      # MongoDB wire protocol serialization strategy for n bytes.
      #
      # Writes and fetches bytes from the byte buffer.
      module Bytes

        # Writes bytes into the buffer.
        #
        # @param [ BSON::ByteBuffer ] buffer Buffer to receive the bytes.
        # @param [ String ] value The bytes to write to the buffer.
        # @param [ true, false ] validating_keys Whether to validate keys.
        #   This option is deprecated and will not be used. It will removed in version 3.0.
        #
        # @return [ BSON::ByteBuffer ] Buffer with serialized value.
        #
        # @since 2.5.0
        def self.serialize(buffer, value, validating_keys = nil)
          buffer.put_bytes(value)
        end

        # Deserializes bytes from the byte buffer.
        #
        # @param [ BSON::ByteBuffer ] buffer Buffer containing the value to read.
        # @param [ Hash ] options The method options.
        #
        # @option options [ Integer ] num_bytes Number of bytes to read.
        #
        # @return [ String ] The bytes.
        #
        # @since 2.5.0
        def self.deserialize(buffer, options = {})
          num_bytes = options[:num_bytes]
          buffer.get_bytes(num_bytes || buffer.length)
        end
      end
    end
  end
end

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
        #
        # @return [ String ] Buffer with serialized value.
        def self.serialize(buffer, value, validating_keys = BSON::Config.validating_keys?)
          buffer.put_bytes(value.pack(HEADER_PACK))
        end

        # Deserializes the header value from the IO stream
        #
        # @param [ String ] buffer Buffer containing the message header.
        #
        # @return [ Array<Fixnum> ] Array consisting of the deserialized
        #   length, request id, response id, and op code.
        def self.deserialize(buffer)
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
        def self.serialize(buffer, value, validating_keys = BSON::Config.validating_keys?)
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
        def self.serialize(buffer, value, validating_keys = BSON::Config.validating_keys?)
          buffer.put_int32(ZERO)
        end
      end

      # MongoDB wire protocol serialization strategy for 32-bit integers.
      #
      # Serializes and de-serializes one 32-bit integer.
      module Int32

        # Serializes a fixnum to a 4-byte 32-bit integer
        #
        # @param buffer [ String ] Buffer to receive the serialized Int32.
        # @param value [ Fixnum ] 32-bit integer to be serialized.
        #
        # @return [String] Buffer with serialized value.
        def self.serialize(buffer, value, validating_keys = BSON::Config.validating_keys?)
          buffer.put_int32(value)
        end

        # Deserializes a 32-bit Fixnum from the IO stream
        #
        # @param [ String ] buffer Buffer containing the 32-bit integer
        #
        # @return [ Fixnum ] Deserialized Int32
        def self.deserialize(buffer)
          buffer.get_int32
        end
      end

      # MongoDB wire protocol serialization strategy for 64-bit integers.
      #
      # Serializes and de-serializes one 64-bit integer.
      module Int64

        # Serializes a fixnum to an 8-byte 64-bit integer
        #
        # @param buffer [ String ] Buffer to receive the serialized Int64.
        # @param value [ Fixnum ] 64-bit integer to be serialized.
        #
        # @return [ String ] Buffer with serialized value.
        def self.serialize(buffer, value, validating_keys = BSON::Config.validating_keys?)
          buffer.put_int64(value)
        end

        # Deserializes a 64-bit Fixnum from the IO stream
        #
        # @param [ String ] buffer Buffer containing the 64-bit integer.
        #
        # @return [Fixnum] Deserialized Int64.
        def self.deserialize(buffer)
          buffer.get_int64
        end
      end

      # MongoDB wire protocol serialization strategy for a Section of OP_MSG.
      #
      # Serializes and de-serializes a list of Sections.
      module Sections

        # Serializes the sections of an OP_MSG, payload type 0 or 1.
        #
        # @param [ String ] buffer Buffer to receive the serialized Sections.
        # @param [ Array<Hash, BSON::Document> ] value The sections to be serialized.
        # @param [ Fixnum ] max_bson_size The max bson size of documents in the sections.
        # @param [ true, false ] validating_keys Whether to validate document keys.
        #
        # @return [ String ] Buffer with serialized value.
        def self.serialize(buffer, value, max_bson_size = nil, validating_keys = BSON::Config.validating_keys?)
          value.each do |section|
            if section[:type] == PayloadZero::TYPE
              PayloadZero.serialize(buffer, section[:payload], max_bson_size, validating_keys)
            elsif section[:type] == PayloadOne::TYPE
              PayloadOne.serialize(buffer, section[:payload], max_bson_size, validating_keys)
            #else raise error?
            end
          end
        end

        # Deserializes a section of an OP_MSG from the IO stream.
        #
        # @param [ String ] buffer Buffer containing the sections.
        #
        # @return [ Array<BSON::Document> ] Deserialized section.
        def self.deserialize(buffer)
          end_size = (@flag_bits & Msg::FLAGS.index(:checksum_present)) == 1 ? 32 : 0

          sections = []
          until buffer.to_s.size == end_size
            case buffer.get_byte
            when PayloadZero::TYPE
              sections << PayloadZero.deserialize(buffer)
            when PayloadOne::TYPE
              sections << PayloadOne.deserialize(buffer)
             # else raise error
            end
          end
        end

        # Whether there can be a size limit on this type after serialization.
        #
        # @return [ true ] Documents can be size limited upon serialization.
        #
        # @since 2.0.0
        def self.size_limited?
          true
        end

        module PayloadZero

          TYPE = 0x0

          TYPE_CSTRING = TYPE.chr.force_encoding(BSON::BINARY).freeze

          # Serializes a section of an OP_MSG, payload type 0.
          #
          # @param [ String ] buffer Buffer to receive the serialized Sections.
          # @param [ Array<Hash, BSON::Document> ] value The sections to be serialized.
          # @param [ Fixnum ] max_bson_size The max bson size of documents in the section.
          # @param [ true, false ] validating_keys Whether to validate document keys.
          #
          # @return [ String ] Buffer with serialized value.
          def self.serialize(buffer, value, max_bson_size = nil, validating_keys = BSON::Config.validating_keys?)
            buffer.put_byte(TYPE_CSTRING)
            Document.serialize(buffer, value, max_bson_size, validating_keys)
          end

          # Deserializes a section of payload type 0 of an OP_MSG from the IO stream.
          #
          # @param [ String ] buffer Buffer containing the sections.
          #
          # @return [ Array<BSON::Document> ] Deserialized section.
          def self.deserialize(buffer)
            BSON::Document.from_bson(buffer)
          end
        end

        module PayloadOne

          TYPE = 0x1

          TYPE_CSTRING = TYPE.chr.force_encoding(BSON::BINARY).freeze

          # Serializes a section of an OP_MSG, payload type 1.
          #
          # @param [ String ] buffer Buffer to receive the serialized Sections.
          # @param [ Array<Hash, BSON::Document> ] value The sections to be serialized.
          # @param [ Fixnum ] max_bson_size The max bson size of documents in the section.
          # @param [ true, false ] validating_keys Whether to validate document keys.
          #
          # @return [ String ] Buffer with serialized value.
          def self.serialize(buffer, value, max_bson_size = nil, validating_keys = BSON::Config.validating_keys?)
            buffer.put_byte(TYPE_CSTRING)
            start = buffer.length
            buffer.put_int32(0) # hold for size
            CString.serialize(buffer, value[:identifier])
            value[:documents].each do |document|
              Document.serialize(buffer, document, max_bson_size, validating_keys)
            end
            buffer.replace_int32(start, buffer.length - start)
          end

          # Deserializes a section of payload type 1 of an OP_MSG from the IO stream.
          #
          # @param [ String ] buffer Buffer containing the sections.
          #
          # @return [ Array<BSON::Document> ] Deserialized section.
          def self.deserialize(buffer)
            start_size = buffer.to_s.size
            section_size = buffer.get_int32 # get the size
            end_size = start_size - section_size
            buffer.get_cstring # get the identifier
            documents = []
            until buffer.to_s.size == end_size
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
        def self.serialize(buffer, value, max_bson_size = nil, validating_keys = BSON::Config.validating_keys?)
          start_size = buffer.length
          value.to_bson(buffer, validating_keys)
          if max_bson_size && buffer.length - start_size > max_bson_size
            raise Error::MaxBSONSize.new(max_bson_size)
          end
        end

        # Deserializes a document from the IO stream
        #
        # @param [ String ] buffer Buffer containing the BSON encoded document.
        #
        # @return [ Hash ] The decoded BSON document.
        def self.deserialize(buffer)
          BSON::Document.from_bson(buffer)
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
    end
  end
end

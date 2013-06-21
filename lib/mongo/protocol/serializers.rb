# Copyright (C) 2013 10gen Inc.
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
        # @param buffer [String] Buffer to receive the serialized value.
        # @param value [String] Header value to be serialized.
        # @return [String] Buffer with serialized value.
        def self.serialize(buffer, value)
          buffer << value.pack(HEADER_PACK)
        end

        # Deserializes the header value from the IO stream
        #
        # @param io [IO] IO stream containing the message header.
        # @return [Array<Fixnum>] Array consisting of the deserialized
        #   length, request id, response id, and op code.
        def self.deserialize(io)
          io.read(16).unpack(HEADER_PACK)
        end
      end

      # MongoDB wire protocol serialization strategy for C style strings.
      #
      # Serializes and de-serializes C style strings (null terminated).
      module CString

        # Serializes a C style string into the buffer
        #
        # @param buffer [String] Buffer to receive the serialized CString.
        # @param value [String] The string to be serialized.
        # @return [String] Buffer with serialized value.
        def self.serialize(buffer, value)
          buffer << value
          buffer << NULL
        end

        # Deserializes a C style string from the IO stream
        #
        # @param io [IO] IO stream containing the CString.
        # @return [String] Deserialized CString.
        def self.deserialize(io)
          io.gets(NULL)
        end
      end

      # MongoDB wire protocol serialization strategy for 32-bit Zero.
      #
      # Serializes and de-serializes one 32-bit Zero.
      module Zero

        # Serializes a 32-bit Zero into the buffer
        #
        # @param buffer [String] Buffer to receive the serialized Zero.
        # @param value [Fixnum] Ignored value.
        # @return [String] Buffer with serialized value.
        def self.serialize(buffer, value)
          buffer << [ZERO].pack(INT32_PACK)
        end

        # Deserializes a 32-bit Zero from the IO stream
        #
        # @param io [IO] IO stream containing the 32-bit Zero.
        # @return [Fixnum] Deserialized Zero.
        def self.deserialize(io)
          io.read(4).unpack(INT32_PACK).first
        end
      end

      # MongoDB wire protocol serialization strategy for 32-bit integers.
      #
      # Serializes and de-serializes one 32-bit integer.
      module Int32

        # Serializes a 32-bit fixnum into the buffer
        #
        # @param buffer [String] Buffer to receive the serialized Int32.
        # @param value [Fixnum] 32-bit integer to be serialized.
        # @return [String] Buffer with serialized value.
        def self.serialize(buffer, value)
          buffer << [value].pack(INT32_PACK)
        end

        # Deserializes a 32-bit Fixnum from the IO stream
        #
        # @param io [IO] IO stream containing the 32-bit integer
        # @return [Fixnum] Deserialized Int32
        def self.deserialize(io)
          io.read(4).unpack(INT32_PACK).first
        end
      end

      # MongoDB wire protocol serialization strategy for 64-bit integers.
      #
      # Serializes and de-serializes one 64-bit integer.
      module Int64

        # Serializes a 64-bit fixnum into the buffer
        #
        # @param buffer [String] Buffer to receive the serialized Int64.
        # @param value [Fixnum] (ZERO) 64-bit integer to be serialized.
        # @return [String] Buffer with serialized value.
        def self.serialize(buffer, value)
          buffer << [value].pack(INT64_PACK)
        end

        # Deserializes a 64-bit Fixnum from the IO stream
        #
        # @param io [IO] IO stream containing the 64-bit integer.
        # @return [Fixnum] Deserialized Int64.
        def self.deserialize(io)
          io.read(8).unpack(INT64_PACK).first
        end
      end

      # MongoDB wire protocol serialization strategy for a BSON Document.
      #
      # Serializes and de-serializes a single document.
      module Document

        # Serializes a document into the buffer
        #
        # @param buffer [String] Buffer to receive the BSON encoded document.
        # @param value [Hash] Document to serialize as BSON.
        # @return [String] Buffer with serialized value.
        def self.serialize(buffer, value)
          value.to_bson(buffer)
        end

        # Deserializes a document from the IO stream
        #
        # @param io [IO] IO stream containing the BSON encoded document.
        # @return [Hash] The decoded BSON document.
        def self.deserialize(io)
          Hash.from_bson(io)
        end
      end
    end
  end
end

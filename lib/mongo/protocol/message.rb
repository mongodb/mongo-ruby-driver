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

    # A base class providing functionality required by all messages in
    # the MongoDB wire protocol. It provides a minimal DSL for defining typed
    # fields to enable serialization and deserialization over the wire.
    #
    # @example
    #   class WireProtocolMessage < Message
    #
    #     private
    #
    #     def op_code
    #       1234
    #     end
    #
    #     FLAGS = [:first_bit, :bit_two]
    #
    #     # payload
    #     field :flags, BitVector.new(FLAGS)
    #     field :namespace, CString
    #     field :document, Document
    #     field :documents, Document, :multi => true
    #   end
    #
    # @abstract
    # @api semiprivate
    class Message
      include Serializers

      # Returns the request id for the message
      #
      # @return [Fixnum] The request id for this message
      attr_reader :request_id

      # Serializes message into bytes that can be sent on the wire
      #
      # @param buffer [String] buffer where the message should be inserted
      # @return [String] buffer containing the serialized message
      def serialize(buffer = ''.force_encoding('BINARY'))
        start = buffer.bytesize
        serialize_header(buffer)
        serialize_fields(buffer)
        length = buffer.bytesize - start
        buffer[start, 4] = Int32.serialize('', length)
        buffer
      end

      alias_method :to_s, :serialize

      # Deserializes messages from an IO stream
      #
      # @param io [IO] Stream containing a message
      # @return [Message] Instance of a Message class
      def self.deserialize(io)
        deserialize_header(io)
        message = allocate
        fields.each do |field|
          if field[:multi]
            deserialize_array(message, io, field)
          else
            deserialize_field(message, io, field)
          end
        end
        message
      end

      private

      @@request_id = 0
      @@id_lock = Mutex.new

      # A method for getting the fields for a message class
      #
      # @return [Integer] the fields for the message class
      def fields
        self.class.fields
      end

      # A class method for getting the fields for a message class
      #
      # @return [Integer] the fields for the message class
      def self.fields
        @fields ||= []
      end

      # Serializes message fields into a buffer
      #
      # @param buffer [String] buffer to receive the field
      # @return [String] buffer with serialized field
      def serialize_fields(buffer)
        fields.each do |field|
          value = instance_variable_get(field[:name])
          if field[:multi]
            value.each do |item|
              field[:type].serialize(buffer, item)
            end
          else
            field[:type].serialize(buffer, value)
          end
        end
      end

      # Generates a request id for a message
      #
      # @return [Fixnum] a request id used for sending a message to the
      #   server. The server will put this id in the response_to field of
      #   a reply.
      def set_request_id
        @@id_lock.synchronize do
          @request_id = @@request_id += 1
        end
      end

      # Serializes the header of the message consisting of 4 32bit integers
      #
      # The integers represent a message length placeholder (calculation of
      # the actual length is deferred) the request id, the response to id,
      # and the op code for the message
      #
      # Currently uses hardcoded 0 for request id and response to as their
      # values are irrelevent to the server
      #
      # @param buffer [String] Buffer to receive the header
      # @return [String] Serialized header
      def serialize_header(buffer)
        set_request_id
        Header.serialize(buffer, [0, request_id, 0, op_code])
      end

      # Deserializes the header of the message
      #
      # @param io [IO] Stream containing the header.
      # @return [Array<Fixnum>] Deserialized header.
      def self.deserialize_header(io)
        @length, @request_id, @response_to, @op_code = Header.deserialize(io)
      end

      # A method for declaring a message field
      #
      # @param name [String] Name of the field
      # @param type [Module] Type specific serialization strategies
      # @param multi [true, false, Symbol] Specify as +true+ to
      #   serialize the field's value as an array of type +:type+ or as a
      #   symbol describing the field having the number of items in the
      #   array (used upon deserialization)
      #
      #     Note: In fields where multi is a symbol representing the field
      #     containing number items in the repetition, the field containing
      #     that information *must* be deserialized prior to deserializing
      #     fields that use the number.
      #
      # @return [NilClass]
      def self.field(name, type, multi = false)
        fields << {
          :name => "@#{name}".intern,
          :type => type,
          :multi => multi
        }

        attr_reader name
      end

      # Deserializes an array of fields in a message
      #
      # The number of items in the array must be described by a previously
      # deserialized field specified in the class by the field dsl under
      # the key +:multi+
      #
      # @param message [Message] Message to contain the deserialized array.
      # @param io [IO] Stream containing the array to deserialize.
      # @param field [Hash] Hash representing a field.
      # @return [Message] Message with deserialized array.
      def self.deserialize_array(message, io, field)
        elements = []
        count = message.instance_variable_get(field[:multi])
        count.times { elements << field[:type].deserialize(io) }
        message.instance_variable_set(field[:name], elements)
      end

      # Deserializes a single field in a message
      #
      # @param message [Message] Message to contain the deserialized field.
      # @param io [IO] Stream containing the field to deserialize.
      # @param field [Hash] Hash representing a field.
      # @return [Message] Message with deserialized field.
      def self.deserialize_field(message, io, field)
        message.instance_variable_set(
          field[:name],
          field[:type].deserialize(io)
        )
      end
    end
  end
end

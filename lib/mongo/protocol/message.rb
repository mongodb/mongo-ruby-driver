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
    #     field :documents, Document, true
    #   end
    #
    # @abstract
    # @api semiprivate
    class Message
      include Id
      include Serializers

      # The batch size constant.
      #
      # @since 2.2.0
      BATCH_SIZE = 'batchSize'.freeze

      # The collection constant.
      #
      # @since 2.2.0
      COLLECTION = 'collection'.freeze

      # The limit constant.
      #
      # @since 2.2.0
      LIMIT = 'limit'.freeze

      # The ordered constant.
      #
      # @since 2.2.0
      ORDERED = 'ordered'.freeze

      # The q constant.
      #
      # @since 2.2.0
      Q = 'q'.freeze

      # Default max message size of 48MB.
      #
      # @since 2.2.1
      MAX_MESSAGE_SIZE = 50331648.freeze

      def initialize(*args) # :nodoc:
        set_request_id
      end

      # Returns the request id for the message
      #
      # @return [Fixnum] The request id for this message
      attr_reader :request_id

      # The default for messages is not to require a reply after sending a
      # message to the server.
      #
      # @example Does the message require a reply?
      #   message.replyable?
      #
      # @return [ false ] The default is to not require a reply.
      #
      # @since 2.0.0
      def replyable?
        false
      end

      # Compress the message, if supported by the wire protocol used and if
      # the command being sent permits compression. Otherwise returns self.
      #
      # @param [ String, Symbol ] compressor The compressor to use.
      # @param [ Integer ] zlib_compression_level The zlib compression level to use.
      #
      # @return [ self ] Always returns self. Other message types should
      #   override this method.
      #
      # @since 2.5.0
      # @api private
      def maybe_compress(compressor, zlib_compression_level = nil)
        self
      end

      # Compress the message, if the command being sent permits compression.
      # Otherwise returns self.
      #
      # @param [ String ] command_name Command name extracted from the message.
      # @param [ String | Symbol ] compressor The compressor to use.
      # @param [ Integer ] zlib_compression_level Zlib compression level to use.
      #
      # @return [ Message ] A Protocol::Compressed message or self,
      #  depending on whether this message can be compressed.
      #
      # @since 2.5.0
      private def compress_if_possible(command_name, compressor, zlib_compression_level)
        if compressor && compression_allowed?(command_name)
          Compressed.new(self, compressor, zlib_compression_level)
        else
          self
        end
      end

      # Inflate a message if it is compressed.
      #
      # @return [ Protocol::Message ] Always returns self. Subclasses should
      #   override this method as necessary.
      #
      # @since 2.5.0
      # @api private
      def maybe_inflate
        self
      end

      # Possibly decrypt this message with libmongocrypt.
      #
      # @param [ Mongo::Operation::Context ] context The operation context.
      #
      # @return [ Mongo::Protocol::Msg ] The decrypted message, or the original
      #   message if decryption was not possible or necessary.
      def maybe_decrypt(context)
        # TODO determine if we should be decrypting data coming from pre-4.2
        # servers, potentially using legacy wire protocols. If so we need
        # to implement decryption for those wire protocols as our current
        # encryption/decryption code is OP_MSG-specific.
        self
      end

      # Possibly encrypt this message with libmongocrypt.
      #
      # @param [ Mongo::Server::Connection ] connection The connection on which
      #   the operation is performed.
      # @param [ Mongo::Operation::Context ] context The operation context.
      #
      # @return [ Mongo::Protocol::Msg ] The encrypted message, or the original
      #   message if encryption was not possible or necessary.
      def maybe_encrypt(connection, context)
        # Do nothing if the Message subclass has not implemented this method
        self
      end

      def maybe_add_server_api(server_api)
        raise Error::ServerApiNotSupported, "Server API parameters cannot be sent to pre-3.6 MongoDB servers. Please remove the :server_api parameter from Client options or use MongoDB 3.6 or newer"
      end

      private def merge_sections
        cmd = if @sections.length > 1
          cmd = @sections.detect { |section| section[:type] == 0 }[:payload]
          identifier = @sections.detect { |section| section[:type] == 1}[:payload][:identifier]
          cmd.merge(identifier.to_sym =>
            @sections.select { |section| section[:type] == 1 }.
              map { |section| section[:payload][:sequence] }.
              inject([]) { |arr, documents| arr + documents }
          )
        elsif @sections.first[:payload]
          @sections.first[:payload]
        else
          @sections.first
        end
        if cmd.nil?
          raise "The command should never be nil here"
        end
        cmd
      end

      # Serializes message into bytes that can be sent on the wire
      #
      # @param buffer [String] buffer where the message should be inserted
      # @return [String] buffer containing the serialized message
      def serialize(buffer = BSON::ByteBuffer.new, max_bson_size = nil, bson_overhead = nil)
        max_size =
          if max_bson_size && bson_overhead
            max_bson_size + bson_overhead
          elsif max_bson_size
            max_bson_size
          else
            nil
          end

        start = buffer.length
        serialize_header(buffer)
        serialize_fields(buffer, max_size)
        buffer.replace_int32(start, buffer.length - start)
      end

      alias_method :to_s, :serialize

      # Deserializes messages from an IO stream.
      #
      # This method returns decompressed messages (i.e. if the message on the
      # wire was OP_COMPRESSED, this method would typically return the OP_MSG
      # message that is the result of decompression).
      #
      # @param [ Integer ] max_message_size The max message size.
      # @param [ IO ] io Stream containing a message
      # @param [ Hash ] options
      #
      # @option options [ Boolean ] :deserialize_as_bson Whether to deserialize
      #   this message using BSON types instead of native Ruby types wherever
      #   possible.
      # @option options [ Numeric ] :socket_timeout The timeout to use for
      #   each read operation.
      #
      # @return [ Message ] Instance of a Message class
      #
      # @api private
      def self.deserialize(io,
        max_message_size = MAX_MESSAGE_SIZE,
        expected_response_to = nil,
        options = {}
      )
        # io is usually a Mongo::Socket instance, which supports the
        # timeout option. For compatibility with whoever might call this
        # method with some other IO-like object, pass options only when they
        # are not empty.
        read_options = {}
        if timeout = options[:socket_timeout]
          read_options[:timeout] = timeout
        end

        if read_options.empty?
          chunk = io.read(16)
        else
          chunk = io.read(16, **read_options)
        end
        buf = BSON::ByteBuffer.new(chunk)
        length, _request_id, response_to, _op_code = deserialize_header(buf)

        # Protection from potential DOS man-in-the-middle attacks. See
        # DRIVERS-276.
        if length > (max_message_size || MAX_MESSAGE_SIZE)
          raise Error::MaxMessageSize.new(max_message_size)
        end

        # Protection against returning the response to a previous request. See
        # RUBY-1117
        if expected_response_to && response_to != expected_response_to
          raise Error::UnexpectedResponse.new(expected_response_to, response_to)
        end

        if read_options.empty?
          chunk = io.read(length - 16)
        else
          chunk = io.read(length - 16, **read_options)
        end
        buf = BSON::ByteBuffer.new(chunk)

        message = Registry.get(_op_code).allocate
        message.send(:fields).each do |field|
          if field[:multi]
            deserialize_array(message, buf, field, options)
          else
            deserialize_field(message, buf, field, options)
          end
        end
        if message.is_a?(Msg)
          message.fix_after_deserialization
        end
        message.maybe_inflate
      end

      # Tests for equality between two wire protocol messages
      # by comparing class and field values.
      #
      # @param other [Mongo::Protocol::Message] The wire protocol message.
      # @return [true, false] The equality of the messages.
      def ==(other)
        return false if self.class != other.class
        fields.all? do |field|
          name = field[:name]
          instance_variable_get(name) ==
            other.instance_variable_get(name)
        end
      end
      alias_method :eql?, :==

      # Creates a hash from the values of the fields of a message.
      #
      # @return [ Fixnum ] The hash code for the message.
      def hash
        fields.map { |field| instance_variable_get(field[:name]) }.hash
      end

      # Generates a request id for a message
      #
      # @return [Fixnum] a request id used for sending a message to the
      #   server. The server will put this id in the response_to field of
      #   a reply.
      def set_request_id
        @request_id = self.class.next_id
      end

      # Default number returned value for protocol messages.
      #
      # @return [ 0 ] This method must be overridden, otherwise, always returns 0.
      #
      # @since 2.5.0
      def number_returned; 0; end

      private

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
      def serialize_fields(buffer, max_bson_size = nil)
        fields.each do |field|
          value = instance_variable_get(field[:name])
          if field[:multi]
            value.each do |item|
              if field[:type].respond_to?(:size_limited?)
                field[:type].serialize(buffer, item, max_bson_size)
              else
                field[:type].serialize(buffer, item)
              end
            end
          else
            if field[:type].respond_to?(:size_limited?)
              field[:type].serialize(buffer, value, max_bson_size)
            else
              field[:type].serialize(buffer, value)
            end
          end
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
        Header.serialize(buffer, [0, request_id, 0, op_code])
      end

      # Deserializes the header of the message
      #
      # @param io [IO] Stream containing the header.
      # @return [Array<Fixnum>] Deserialized header.
      def self.deserialize_header(io)
        Header.deserialize(io)
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
      # @param options [ Hash ]
      #
      # @option options [ Boolean ] :deserialize_as_bson Whether to deserialize
      #   each of the elements in this array using BSON types wherever possible.
      #
      # @return [Message] Message with deserialized array.
      def self.deserialize_array(message, io, field, options = {})
        elements = []
        count = message.instance_variable_get(field[:multi])
        count.times { elements << field[:type].deserialize(io, options) }
        message.instance_variable_set(field[:name], elements)
      end

      # Deserializes a single field in a message
      #
      # @param message [Message] Message to contain the deserialized field.
      # @param io [IO] Stream containing the field to deserialize.
      # @param field [Hash] Hash representing a field.
      # @param options [ Hash ]
      #
      # @option options [ Boolean ] :deserialize_as_bson Whether to deserialize
      #   this field using BSON types wherever possible.
      #
      # @return [Message] Message with deserialized field.
      def self.deserialize_field(message, io, field, options = {})
        message.instance_variable_set(
          field[:name],
          field[:type].deserialize(io, options)
        )
      end
    end
  end
end

module Mongo
  module Protocol

    # An abstract class providing functionality required by all messages in 
    # the MongoDB wire protocol. It provides a minimal DSL for defining typed
    # fields to enable serialization and deserialization over the wire.
    #
    # Messages produce a new request id upon serialization and can be
    # serialized multiple times in order to repeat operations.
    #
    # @example
    #
    #   class WireProtocolMessage < Message
    #
    #     private
    #
    #     OP_CODE = 1234
    #     FLAGS = [ :first_bit, :bit_two ]
    #
    #     # header
    #     field :length, Int32
    #     field :request_id, Int32
    #     field :response_to, Int32
    #     field :op_code, Int32
    #
    #     # payload
    #     field :flags, BitVector.new(FLAGS)
    #     field :namespace, CString
    #     field :document, Document
    #     field :documents, Document, :multi => true
    #   end
    #
    # All messages classes *must* contain the message header as required by
    # the MongoDB wire protocol as the first four fields, namely:
    #
    #   field :length, Int32
    #   field :request_id, Int32
    #   field :response_to, Int32
    #   field :op_code, Int32
    #
    class Message
      include Serializers

      # Method to serialize the message into bytes that can be sent on
      # the wire.
      #
      # @param buffer [String] buffer where the message should be inserted
      # @return [String] buffer containing the serialized message
      def serialize(buffer = ''.force_encoding('BINARY'))
        start = buffer.bytesize
        serialize_fields(buffer)
        length = buffer.bytesize - start
        buffer[start, 4] = Int32::serialize('', length)
        buffer
      end

      alias :to_s :serialize

      # Deserializes messages from an IO stream.
      #
      # @param [IO] Stream containing a message.
      # @return [Message] Instance of a Message class.
      def self.deserialize(io)
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

      protected

      # Serializes message fields into a buffer.
      #
      # @param [String] the buffer
      def serialize_fields(buffer)
        fields.each do |field|
          value = instance_variable_get(field[:name])
          if field[:multi]
            value.each do |item|
              field[:type]::serialize(buffer, item)
            end
          else
            field[:type]::serialize(buffer, value)
          end
        end
      end

      # Class variables for atomic request id
      @@request_id = 0
      @@id_lock = Mutex.new

      # Generates a request id for a message.
      #
      # @return [Fixnum] a request id used for sending a message to the
      #   server. The server will put this id in the response_to field of
      #   a reply.
      def request_id
        request_id = nil
        @@id_lock.synchronize do
          request_id = @@request_id += 1
        end
        request_id
      end

      # A method for getting the message class OP_CODE
      #
      # @return [Integer] the opcode
      def op_code
        self.class::OP_CODE
      end

      # An method for getting the fields for a message class
      #
      # @return [Integer] the fields for the message class
      def fields
        self.class.fields
      end

      # An method for getting the fields for a message class
      #
      # @return [Integer] the fields for the message class
      def self.fields
        @fields ||= []
      end

      # A method for declaring a message field.
      #
      # @param name [String] Name of the field.
      # @param type [Module] Type specific serialization strategies.
      # @param options [Hash] The additional field options.
      #
      # @option options :multi [ Boolean, Symbol ] Specify as +true+ to
      #   serialize the field's value as an array of type +:type+ or as a
      #   symbol describing the field having the number of items in the
      #   array (used upon deserialization).
      #
      #     Note: In fields where multi is a symbol representing the field
      #     containing number items in the repetition, the field containing
      #     that information *must* be deserialized prior to deserializing
      #     fields that use the number.
      def self.field(name, type, options = {})
        fields << {
          :name => "@#{name}".intern,
          :type => type,
          :multi => options[:multi]
        }

        attr_accessor name
      end

      # Deserializes an array of fields in a message.
      #
      # The number of items in the array must be described by a previously
      # deserialized field specified in the class by the field dsl under
      # the key +:multi+.
      #
      # @param message [Message] Message to contain the deserialized array.
      # @param io [IO] Stream containing the array to deserialize.
      # @param field [Hash] Hash representing a field.
      def self.deserialize_array(message, io, field)
        elements = []
        count = message.instance_variable_get(field[:multi])
        count.times { elements << field[:type]::deserialize(io) }
        message.instance_variable_set(field[:name], elements)
      end

      # Deserializes a single field in a message.
      #
      # @param message [Message] Message to contain the deserialized field.
      # @param io [IO] Stream containing the field to deserialize.
      # @param field [Hash] Hash representing a field.
      def self.deserialize_field(message, io, field)
        message.instance_variable_set(
          field[:name],
          field[:type]::deserialize(io)
        )
      end
    end
  end
end

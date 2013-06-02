module Mongo
  module Protocol
    # Class used to define a bitvector for a MongoDB wire protocol message.
    #
    # Defines serialization strategy based upon initialization.
    class BitVector

      # Initializes a BitVector with a layout.
      #
      # @param layout [Array<Symbol>] the array of fields in the bit vector.
      def initialize(layout)
        @masks = {}
        layout.each_with_index do |field, index|
          @masks[field] = 2 ** index
        end
      end

      # Serializes vector by encoding each symbol according to it's mask.
      #
      # @param buffer [IO] the buffer to receive the serialized vector.
      # @param value [Array<Symbol>] the array of flags to encode.
      def serialize(buffer, value)
        bits = 0
        value.each { |flag| bits |= @masks[flag] }
        buffer << [bits].pack(INT32_PACK)
      end

      # Deserializes vector by decoding the symbol according to its mask.
      #
      # @param [IO] the stream containing the vector to be deserialized.
      def deserialize(io)
        vector = io.read(4).unpack(INT32_PACK).first
        flags = []
        @masks.each do |flag, mask|
          flags << flag if mask & vector != 0
        end
        flags
      end
    end
  end
end

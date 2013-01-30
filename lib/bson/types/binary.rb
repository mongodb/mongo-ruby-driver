require 'bson/byte_buffer'

module BSON

  # An array of binary bytes with a MongoDB subtype. See the subtype
  # constants for reference.
  #
  # Use this class when storing binary data in documents.
  class Binary < ByteBuffer

    SUBTYPE_SIMPLE       = 0x00
    SUBTYPE_BYTES        = 0x02
    SUBTYPE_UUID         = 0x03
    SUBTYPE_MD5          = 0x05
    SUBTYPE_USER_DEFINED = 0x80

    # One of the SUBTYPE_* constants. Default is SUBTYPE_BYTES.
    attr_accessor :subtype

    # Create a buffer for storing binary data in MongoDB.
    #
    # @param [Array, String] data to story as BSON binary. If a string is given, the on
    #   Ruby 1.9 it will be forced to the binary encoding.
    # @param [Fixnum] one of four values specifying a BSON binary subtype. Possible values are
    #   SUBTYPE_BYTES, SUBTYPE_UUID, SUBTYPE_MD5, and SUBTYPE_USER_DEFINED.
    #
    # @see http://www.mongodb.org/display/DOCS/BSON#BSON-noteondatabinary BSON binary subtypes.
    def initialize(data=[], subtype=SUBTYPE_SIMPLE)
      super(data)
      @subtype = subtype
    end

    def inspect
      "<BSON::Binary:#{object_id}>"
    end

  end
end

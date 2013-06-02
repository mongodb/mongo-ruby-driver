module Mongo
  module Protocol

    # Container for various serialization strategies.
    #
    # Each strategy must have a serialization method named +serailize+
    # and a deserialization method named +deserialize+.
    #
    # Serialize methods must take buffer and value arguements and
    # serialize the value into the buffer.
    #
    # Deserialize methods must take an IO stream argument and
    # deserialize the value from the stream of bytes.
    module Serializers
      private

      NULL = 0.chr.freeze
      INT32_PACK = 'l<'.freeze
      INT64_PACK = 'q<'.freeze

      module CString
        def self.serialize(buffer, value)
          buffer << value
          buffer << NULL
        end

        def self.deserialize(io)
          io.gets(NULL)
        end
      end

      module Int32
        def self.serialize(buffer, value)
          buffer << [value].pack(INT32_PACK)
        end

        def self.deserialize(io)
          io.read(4).unpack(INT32_PACK).first
        end
      end

      module Int64
        def self.serialize(buffer, value)
          buffer << [value].pack(INT64_PACK)
        end

        def self.deserialize(io)
          io.read(8).unpack(INT64_PACK).first
        end
      end

      module Document
        def self.serialize(buffer, value)
          value.to_bson(buffer)
        end

        def self.deserialize(io)
          Hash.from_bson(io)
        end
      end
    end
  end
end

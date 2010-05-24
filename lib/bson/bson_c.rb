# encoding: UTF-8

# A thin wrapper for the CBson class
module BSON
  class BSON_C

    def self.serialize(obj, check_keys=false, move_id=false)
      ByteBuffer.new(CBson.serialize(obj, check_keys, move_id))
    end

    def self.deserialize(buf=nil)
      if buf.is_a? String
        buf = ByteBuffer.new(buf) if buf
      else
        buf = ByteBuffer.new(buf.to_a) if buf
      end
      buf.rewind
      CBson.deserialize(buf.to_s)
    end

  end
end

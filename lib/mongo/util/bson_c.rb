# A thin wrapper for the CBson class
class BSON_C

  def self.serialize(obj, check_keys=false)
    ByteBuffer.new(CBson.serialize(obj, check_keys))
  end

  def self.deserialize(buf=nil)
    if buf.is_a? String
      to_deserialize = ByteBuffer.new(buf) if buf
    else
      buf = ByteBuffer.new(buf.to_a) if buf
    end
    buf.rewind
    CBson.deserialize(buf.to_s)
  end

end

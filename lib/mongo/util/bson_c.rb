# A thin wrapper for the CBson class
class BSON_C

  if RUBY_VERSION >= '1.9'
    def self.to_utf8(str)
      str.encode("utf-8")
    end
  else
    def self.to_utf8(str)
      begin
      str.unpack("U*")
      rescue => ex
        raise InvalidStringEncoding, "String not valid utf-8: #{str}"
      end
      str
    end
  end

  def self.serialize_cstr(buf, val)
    buf.put_array(to_utf8(val.to_s).unpack("C*") + [0])
  end

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

  def deserialize(buf=nil)
    self.class.deserialize(buf)
  end

  def serialize(buf, check_keys=false)
    self.class.serialize(buf, check_keys)
  end

end

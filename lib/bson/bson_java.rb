include Java
module BSON
  class BSON_JAVA

    def self.serialize(obj, check_keys=false, move_id=false)
      raise InvalidDocument, "BSON_JAVA.serialize takes a Hash" unless obj.is_a?(Hash)
      enc = Java::OrgJbson::RubyBSONEncoder.new(JRuby.runtime)
      ByteBuffer.new(enc.encode(obj))
    end

    def self.get_encoder
      @@enc ||= Java::OrgJbson::RubyBSONEncoder.new(JRuby.runtime)
    end

    def self.get_decoder
      @@dec ||= Java::OrgBson::BSONDecoder.new
    end

    def self.deserialize(buf)
      dec = Java::OrgBson::BSONDecoder.new
      callback = Java::OrgJbson::RubyBSONJavaCallback.new(JRuby.runtime)
      dec.decode(buf.to_s.to_java_bytes, callback)
      callback.get
    end

  end
end

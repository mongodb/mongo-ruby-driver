require 'jruby'

include Java

jar_dir = File.join(File.dirname(__FILE__), '../../ext/jbson')
require File.join(jar_dir, 'lib/java-bson.jar')
require File.join(jar_dir, 'target/jbson.jar')

module BSON
  class BSON_JAVA
    def self.serialize(obj, check_keys=false, move_id=false, max_bson_size=DEFAULT_MAX_BSON_SIZE)
      raise InvalidDocument, "BSON_JAVA.serialize takes a Hash" unless obj.is_a?(Hash)
      enc = Java::OrgJbson::RubyBSONEncoder.new(JRuby.runtime, check_keys, move_id, max_bson_size)
      ByteBuffer.new(enc.encode(obj))
    end

    def self.deserialize(buf)
      dec = Java::OrgJbson::RubyBSONDecoder.new
      callback = Java::OrgJbson::RubyBSONCallback.new(JRuby.runtime)
      dec.decode(buf.to_s.to_java_bytes, callback)
      callback.get
    end

    def self.max_bson_size
      warn "BSON::BSON_CODER.max_bson_size is deprecated and will be removed in v2.0."
      Java::OrgJbson::RubyBSONEncoder.max_bson_size(self)
    end

    def self.update_max_bson_size(connection)
      warn "BSON::BSON_CODER.update_max_bson_size is deprecated and now a no-op. It will be removed in v2.0."
      Java::OrgJbson::RubyBSONEncoder.update_max_bson_size(self, connection)
    end
  end
end

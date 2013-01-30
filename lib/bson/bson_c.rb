# A thin wrapper for the CBson class
module BSON
  class BSON_C

    def self.serialize(obj, check_keys=false, move_id=false, max_bson_size=DEFAULT_MAX_BSON_SIZE)
      ByteBuffer.new(CBson.serialize(obj, check_keys, move_id, max_bson_size))
    end

    def self.deserialize(buf=nil)
      CBson.deserialize(ByteBuffer.new(buf).to_s)
    end

    def self.max_bson_size
      warn "BSON::BSON_CODER.max_bson_size is deprecated and will be removed in v2.0."
      CBson.max_bson_size
    end

    def self.update_max_bson_size(connection)
      warn "BSON::BSON_CODER.update_max_bson_size is deprecated and now a no-op. It will be removed in v2.0."
      CBson.update_max_bson_size(connection)
    end
  end
end

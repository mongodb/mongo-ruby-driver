# Copyright (C) 2009-2013 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# A thin wrapper for the BSON C-Extension
module BSON
  class BSON_C

    def self.serialize(obj, check_keys=false, move_id=false, max_bson_size=DEFAULT_MAX_BSON_SIZE)
      ByteBuffer.new(CBson.serialize(obj, check_keys, move_id, max_bson_size))
    end

    def self.deserialize(buf=nil, opts={})
      CBson.deserialize(ByteBuffer.new(buf).to_s, opts)
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

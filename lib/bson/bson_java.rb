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

require 'jruby'

include Java

jar_dir = File.expand_path(File.join(File.dirname(__FILE__), '../../ext/jbson'))
require File.join(jar_dir, 'lib/java-bson.jar')
require File.join(jar_dir, 'target/jbson.jar')

module BSON
  class BSON_JAVA
    def self.serialize(obj, check_keys=false, move_id=false, max_bson_size=DEFAULT_MAX_BSON_SIZE)
      raise InvalidDocument, "BSON_JAVA.serialize takes a Hash" unless obj.is_a?(Hash)
      enc = Java::OrgJbson::RubyBSONEncoder.new(JRuby.runtime, check_keys, move_id, max_bson_size)
      ByteBuffer.new(enc.encode(obj))
    end

    def self.deserialize(buf, opts={})
      dec = Java::OrgJbson::RubyBSONDecoder.new
      callback = Java::OrgJbson::RubyBSONCallback.new(JRuby.runtime)
      callback.set_opts(opts);
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

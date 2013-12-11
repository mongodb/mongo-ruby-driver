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

module BSON
  DEFAULT_MAX_BSON_SIZE = 4 * 1024 * 1024

  def self.serialize(obj, check_keys=false, move_id=false)
    BSON_CODER.serialize(obj, check_keys, move_id)
  end

  def self.deserialize(buf=nil, opts={})
    BSON_CODER.deserialize(buf, opts)
  end

  # Reads a single BSON document from an IO object.
  # This method is used in the executable b2json, bundled with
  # the bson gem, for reading a file of bson documents.
  #
  # @param [IO] io an io object containing a bson object.
  #
  # @return [ByteBuffer]
  def self.read_bson_document(io)
    bytebuf = BSON::ByteBuffer.new
    sz = io.read(4).unpack("V")[0]
    bytebuf.put_int(sz)
    bytebuf.put_array(io.read(sz-4).unpack("C*"))
    bytebuf.rewind
    return BSON.deserialize(bytebuf)
  end

  def self.extension?
    !((ENV.key?('BSON_EXT_DISABLED') && RUBY_PLATFORM =~ /java/) ||
      (ENV.key?('BSON_EXT_DISABLED') || "\x01\x00\x00\x00".unpack("i")[0] != 1))
  end
end

begin
  # Skips loading extensions if one of the following is true:
  # 1) JRuby and BSON_EXT_DISABLED is set.
  #     -OR-
  # 2) Ruby MRI and big endian or BSON_EXT_DISABLED is set.
  raise LoadError unless BSON.extension?

  if RUBY_PLATFORM =~ /java/
    require 'bson/bson_java'
    module BSON
      BSON_CODER = BSON_JAVA
    end
  else
    require 'bson_ext/cbson'
    raise LoadError unless defined?(CBson::VERSION)
    require 'bson/bson_c'
    module BSON
      BSON_CODER = BSON_C
    end
  end
rescue LoadError
  require 'bson/bson_ruby'
  module BSON
    BSON_CODER = BSON_RUBY
  end

  if RUBY_PLATFORM =~ /java/
    unless ENV['TEST_MODE']
      warn <<-NOTICE
      ** Notice: The BSON extension was not loaded. **

      For optimal performance, use of the BSON extension is recommended. To
      enable the extension make sure ENV['BSON_EXT_DISABLED'] is not set.
      NOTICE
    end
  else
    unless ENV['TEST_MODE']
      warn <<-NOTICE
      ** Notice: The native BSON extension was not loaded. **

      For optimal performance, use of the BSON extension is recommended.

      To enable the extension make sure ENV['BSON_EXT_DISABLED'] is not set
      and run the following command:

        gem install bson_ext

      If you continue to receive this message after installing, make sure that
      the bson_ext gem is in your load path.
      NOTICE
    end
  end
end

require 'base64'
require 'bson/bson_ruby'
require 'bson/byte_buffer'
require 'bson/exceptions'
require 'bson/ordered_hash'
require 'bson/types/binary'
require 'bson/types/code'
require 'bson/types/dbref'
require 'bson/types/min_max_keys'
require 'bson/types/regex'
require 'bson/types/object_id'
require 'bson/types/timestamp'

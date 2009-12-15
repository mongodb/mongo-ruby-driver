# --
# Copyright (C) 2008-2009 10gen Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ++

require 'mongo/types/objectid'
require 'mongo/util/byte_buffer'
require 'mongo/util/ordered_hash'

module GridFS

  # A chunk stores a portion of GridStore data.
  class Chunk

    DEFAULT_CHUNK_SIZE = 1024 * 256

    attr_reader :object_id, :chunk_number
    attr_accessor :data

    def initialize(file, mongo_object={})
      @file = file
      @object_id = mongo_object['_id'] || Mongo::ObjectID.new
      @chunk_number = mongo_object['n'] || 0

      @data = ByteBuffer.new
      case mongo_object['data']
      when String
        mongo_object['data'].each_byte { |b| @data.put(b) }
      when ByteBuffer
        @data.put_array(mongo_object['data'].to_a)
      when Array
        @data.put_array(mongo_object['data'])
      when nil
      else
        raise "illegal chunk format; data is #{mongo_object['data'] ? (' ' + mongo_object['data'].class.name) : 'nil'}"
      end
      @data.rewind
    end

    def pos; @data.position; end
    def pos=(pos); @data.position = pos; end
    def eof?; !@data.more?; end

    def size; @data.size; end
    alias_method :length, :size

    def truncate
      if @data.position < @data.length
        curr_data = @data
        @data = ByteBuffer.new
        @data.put_array(curr_data.to_a[0...curr_data.position])
      end
    end

    def getc
      @data.more? ? @data.get : nil
    end

    def putc(byte)
      @data.put(byte)
    end

    def save
      coll = @file.chunk_collection
      coll.remove({'_id' => @object_id})
      coll.insert(to_mongo_object)
    end

    def to_mongo_object
      h = OrderedHash.new
      h['_id'] = @object_id
      h['files_id'] = @file.files_id
      h['n'] = @chunk_number
      h['data'] = data
      h
    end

  end
end

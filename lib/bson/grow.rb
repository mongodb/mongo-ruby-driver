# encoding: utf-8

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
  module Grow
    # module with methods to grow BSON docs/objects/arrays
    # this module is intended for internal use and is subject to change
    # proper usage is essential as minimal overhead is preferred over usage checks
    # unfinish! returns unfinished BSON for faster growing with bang! methods
    # bang! methods work on unfinished BSON with neither terminating nulls nor proper sizes
    # finish! must be called to finish BSON after using bang! methods
    # corresponding non-bang methods work on finished BSON
    # object/array methods should be paired, ex., array!/b_end! and array/b_end
    # push!/push and push_doc!/push_doc append to arrays with correct keys
    # b_end needs a better name

    def to_e # Extract bytes for elements from BSON
      @str[4...-1]
    end

    def to_t # Extract type from (single-element) BSON
      @str[4,1]
    end

    def to_v # Extract value from (single-element) BSON
      @str[(@str.index(NULL_BYTE,5)+1)...-1]
    end

    def finish_one!(offset = 0) # Appends terminating null byte and sets size
      put(0)
      put_int(@str.size - offset, offset)
      @cursor = @str.size
      self
    end

    def unfinish! # Backup past terminating null bytes
      @b_pos ||= [0]
      @cursor = @str.size - @b_pos.size # BSON::BSON_CODER.serialize may not restore @cursor
      self
    end

    def finish! # Append all terminating null bytes and set all sizes
      @b_pos ||= [0]
      (@b_pos.size-1).downto(0){|i| finish_one!(@b_pos[i])}
      self
    end

    def grow!(bson) # Appends BSON elements unfinished
      put_binary(bson.to_e)
      self
    end

    def grow(bson) # Appends BSON elements finished
      @b_pos ||= [0]
      put_binary(bson.to_e, @str.size - @b_pos.size)
      finish!
    end

    def push!(bson) # Appends BSON element value with correct key unfinished
      @a_index ||= [0]
      @b_pos ||= [0]
      put_binary(bson.to_t)
      put_binary(@a_index[-1].to_s)
      put_binary(NULL_BYTE)
      @a_index[-1] += 1
      put_binary(bson.to_v)
      self
    end

    def push(bson) # Appends BSON element value with correct key finished
      @a_index ||= [0]
      @b_pos ||= [0]
      put_binary(bson.to_t, @str.size - @b_pos.size)
      put_binary(@a_index[-1].to_s)
      put_binary(NULL_BYTE)
      @a_index[-1] += 1
      put_binary(bson.to_v)
      finish!
    end

    def push_doc!(bson) # Appends BSON doc with correct key unfinished
      @a_index ||= [0]
      @b_pos ||= [0]
      put(BSON::BSON_RUBY::OBJECT)
      put_binary(@a_index[-1].to_s)
      put(0)
      @a_index[-1] += 1
      put_binary(bson.to_s)
      self
    end

    def push_doc(bson) # Appends BSON doc with correct key finished
      @a_index ||= [0]
      @b_pos ||= [0]
      put(BSON::BSON_RUBY::OBJECT, @str.size - @b_pos.size)
      put_binary(@a_index[-1].to_s)
      put(0)
      @a_index[-1] += 1
      put_binary(bson.to_s)
      finish!
    end

    def b_do!(key, type = BSON::BSON_RUBY::OBJECT) # Append object/array element unfinished
      put(type)
      BSON::BSON_RUBY.serialize_cstr(self, key)
      @b_pos ||= [0]
      @a_index ||= [0]
      @b_pos << @cursor # mark position of size
      @a_index << 0
      put_int(0)
      self
    end

    def b_do(key, type = BSON::BSON_RUBY::OBJECT) # Append object/array element finished
      @b_pos ||= [0]
      @cursor = @str.size - @b_pos.size
      b_do!(key, type)
      finish!
    end

    def doc!(key) # Append object element unfinished
      b_do!(key, BSON::BSON_RUBY::OBJECT)
    end

    def doc(key) # Append object element finished
      b_do(key, BSON::BSON_RUBY::OBJECT)
    end

    def array!(key) # Append array element unfinished
      b_do!(key, BSON::BSON_RUBY::ARRAY)
    end

    def array(key) # Append array element finished
      b_do(key, BSON::BSON_RUBY::ARRAY)
    end

    def b_end! # End object/array unfinished - next operation will be up one level
      @b_pos ||= [0]
      finish_one!(@b_pos[-1])
      @b_pos.pop
      @a_index ||= [0]
      @a_index.pop
      self
    end

    def b_end # End object/array finished - next operation will be up one level
      @b_pos ||= [0]
      @b_pos.pop
      @a_index ||= [0]
      @a_index.pop
      self
    end

    def clear! # Clear internal state for reuse
      @b_pos = @a_index = nil
      self
    end

  end
end

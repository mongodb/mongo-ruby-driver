# Copyright (C) 2019 MongoDB, Inc.
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

require 'ffi'

module Mongo
  module Crypt

    # A wrapper around mongocrypt_binary_t, a non-owning buffer of
    # uint-8 byte data. Each Binary instance keeps a copy of the data
    # passed to it in order to keep that data alive.
    class Binary
      # Create a new Binary object that wraps a byte string
      #
      # @param [ String ] data The data string wrapped by the
      #   byte buffer (optional)
      def initialize(data=nil)
        if data
          # Represent data string as array of uint-8 bytes
          bytes = data.unpack('C*')

          # FFI::MemoryPointer automatically frees memory when it goes out of scope
          @data_p = FFI::MemoryPointer.new(bytes.length)
                    .write_array_of_uint8(bytes)

          # FFI::AutoPointer uses a custom release strategy to automatically free
          # the pointer once this object goes out of scope
          @bin = FFI::AutoPointer.new(
            Binding.mongocrypt_binary_new_from_data(@data_p, bytes.length)
            Binding.method(:mongocrypt_binary_destroy)
          )
        else
          # FFI::AutoPointer uses a custom release strategy to automatically free
          # the pointer once this object goes out of scope
          @bin = FFI::AutoPointer.new(
            Binding.mongocrypt_binary_new,
            Binding.method(:mongocrypt_binary_destroy)
          )
        end
      end

      # Returns the data stored as a byte array
      #
      # @return [ Array<Int> ] Byte array stored in mongocrypt_binary_t
      def to_bytes
        data = Binding.mongocrypt_binary_data(@bin)
        if data == FFI::Pointer::NULL
          return []
        end

        len = Binding.mongocrypt_binary_len(@bin)
        data.get_array_of_uint8(0, len)
      end

      # Returns the data stored as a string
      #
      # @return [ String ] Data stored in the mongocrypt_binary_t as a string
      def to_string
        to_bytes.pack('C*')
      end

      # Returns the reference to the underlying mongocrypt_binary_t
      # object
      #
      # @return [ FFI::Pointer ] The underlying mongocrypt_binary_t object
      def ref
        @bin
      end
    end
  end
end

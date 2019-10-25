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
  module Libmongocrypt

    # A wrapper around mongocrypt_binary_t, a non-owning buffer of
    # uint-8 byte data. Each Binary instance keeps a copy of the data
    # passed to it in order to keep that data alive.
    #
    # @since 2.12.0
    class Binary

      # Create a new Binary object that wraps an array of bytes
      #
      # @example Instantiate a Binary object
      #   Mongo::Libmongocrypt::Binary.new([73, 76, 111, 118, 101, 82, 117, 98, 121])
      #
      # @param [ Array<Int> ] data An array of uint-8 bytes
      #
      # @since 2.12.0
      def initialize(data)
        unless data
          raise MongocryptError.new('Cannot create new Binary object with no data.')
        end

        # FFI::MemoryPointer automatically frees memory when it goes out of scope
        @data_p = FFI::MemoryPointer.new(data.length)
                  .write_array_of_type(FFI::TYPE_UINT8, :put_uint8, data)

        @bin = Binding.mongocrypt_binary_new_from_data(@data_p, data.length)
      end

      # Returns the data stored as a byte array
      #
      # @return [ Array<Int> ] Byte array stored in mongocrypt_binary_t
      #
      # @since 2.12.0
      def to_bytes
        data = Binding.mongocrypt_binary_data(@bin)
        if data == FFI::Pointer::NULL
          return []
        end

        len = Binding.mongocrypt_binary_len(@bin)
        data.read_array_of_type(FFI::TYPE_UINT8, :read_uint8, len)
      end

      # Releases allocated memory and cleans up resources
      #
      # @return [ true ] Always true.
      #
      # @since 2.12.0
      def close
        Binding.mongocrypt_binary_destroy(@bin) if @bin

        @data_p = nil
        @bin = nil

        true
      end
    end
  end
end

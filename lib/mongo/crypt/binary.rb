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
    #
    # @since 2.12.0
    class Binary
      attr_accessor :bin

      # Create a new Binary object that wraps a string
      #
      # @example Instantiate a Binary object
      #   Mongo::Crypt::Binary.new('Hello, world!')
      #
      # @param [ String ] data
      #
      # @since 2.12.0
      def initialize(data)
        unless data
          raise ArgumentError.new('Cannot create new Binary object with no data')
        end

        # Represent data string as array of uint-8 bytes
        bytes = data.unpack("C*")

        # FFI::MemoryPointer automatically frees memory when it goes out of scope
        @data_p = FFI::MemoryPointer.new(bytes.length)
                  .write_array_of_type(FFI::TYPE_UINT8, :put_uint8, bytes)

        @bin = Binding.mongocrypt_binary_new_from_data(@data_p, bytes.length)
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

      def ref
        @bin
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

      # Convenient API for using binary object without having
      # to perform cleanup.
      #
      # @example
      #   Mongo::Crypt::Binary.with_binary('Hello, world!') do |binary|
      #     binary.to_bytes # => [72, 101, 108, 108, 111, 44, 32, 119, 111, 114, 108, 100, 33]
      #   end
      #
      # @since 2.12.0
      def self.with_binary(data)
        binary = self.new(data)
        begin
          yield(binary)
        ensure
          binary.close
        end
      end
    end
  end
end

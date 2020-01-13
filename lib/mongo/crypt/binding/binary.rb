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
    class Binding

      # @api private
      class Binary
        class << self
          # Write data to a Binary object
          #
          # @param [ Mongo::Crypt::Binary ] binary A Binary object
          # @param [ String ] data The data to write to the binary object
          #
          # @return [ true ] Always true
          # @raise [ ArgumentError ] Raises when trying to write more data
          # than was originally allocated
          def write(binary, data)
            binary_p = binary.ref

            # Cannot write a string that's longer than the space currently allocated
            # by the mongocrypt_binary_t object
            data_p = Binding.mongocrypt_binary_data(binary_p)
            len = Binding.mongocrypt_binary_len(binary_p)

            if len < data.length
              raise ArgumentError.new(
                "Cannot write #{data.length} bytes of data to a Binary object " +
                "that was initialized with #{len} bytes."
              )
            end

            data_p.put_bytes(0, data)

            true
          end

          # Return the data referenced by the mongocrypt_binary_t object
          # as a string
          #
          # @param [ Mongo::Crypt::Binary ] binary A Binary object
          #
          # @return [ String ] The underlying byte data as a string
          def to_s(binary)
            binary_p = binary.ref

            str_p = Binding.mongocrypt_binary_data(binary_p)
            len = Binding.mongocrypt_binary_len(binary_p)
            str_p.read_string(len)
          end

          # Initialize a mongocrypt_binary_t object that references the
          #   specified string
          #
          # @param [ String ] string The string to be wrapped by the
          #   mongocryt_binary_t
          #
          # @return [ FFI::AutoPointer ] A pointer to the new
          #   mongocrypt_binary_t object, which will be automatically cleaned
          #   up when this object goes out of scope
          def with_data(string)
            bytes = string.unpack('C*')

            # FFI::MemoryPointer automatically frees memory when it goes out of scope
            data_p = FFI::MemoryPointer
              .new(bytes.size)
              .write_array_of_type(FFI::TYPE_UINT8, :put_uint8, bytes)

            # FFI::AutoPointer uses a custom release strategy to automatically free
            # the pointer once this object goes out of scope
            FFI::AutoPointer.new(
              Binding.mongocrypt_binary_new_from_data(data_p, bytes.length),
              Binding.method(:mongocrypt_binary_destroy)
            )
          end

          def data_p(binary)
            binary_p = binary.ref
            Binding.mongocrypt_binary_data(binary_p)
          end

          # Create a new pointer to a mongocrypt_binary_t object that wraps no
          #   data
          #
          # @return [ FFI::AutoPointer ] A pointer to the new
          #   mongocrypt_binary_t object, which will be automatically cleaned
          #   up when it goes out of scope
          def without_data
            # FFI::AutoPointer uses a custom release strategy to automatically free
            # the pointer once this object goes out of scope
            FFI::AutoPointer.new(
              Binding.mongocrypt_binary_new,
              Binding.method(:mongocrypt_binary_destroy)
            )
          end
        end
      end
    end
  end
end

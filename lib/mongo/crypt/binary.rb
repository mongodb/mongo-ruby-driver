# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2019-2020 MongoDB Inc.
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
    # @api private
    class Binary
      # Create a new Binary object that wraps a byte string
      #
      # @param [ String ] data The data string wrapped by the
      #   byte buffer (optional)
      # @param [ FFI::Pointer ] pointer A pointer to an existing
      #   mongocrypt_binary_t object
      #
      # @note When initializing a Binary object with a string or a pointer,
      # it is recommended that you use #self.from_pointer or #self.from_data
      # methods
      def initialize(data: nil, pointer: nil)
        if data
          # Represent data string as array of uint-8 bytes
          bytes = data.unpack('C*')

          # FFI::MemoryPointer automatically frees memory when it goes out of scope
          @data_p = FFI::MemoryPointer.new(bytes.length)
                    .write_array_of_uint8(bytes)

          # FFI::AutoPointer uses a custom release strategy to automatically free
          # the pointer once this object goes out of scope
          @bin = FFI::AutoPointer.new(
            Binding.mongocrypt_binary_new_from_data(@data_p, bytes.length),
            Binding.method(:mongocrypt_binary_destroy)
          )
        elsif pointer
          # If the Binary class is used this way, it means that the pointer
          # for the underlying mongocrypt_binary_t object is allocated somewhere
          # else. It is not the responsibility of this class to de-allocate data.
          @bin = pointer
        else
          # FFI::AutoPointer uses a custom release strategy to automatically free
          # the pointer once this object goes out of scope
          @bin = FFI::AutoPointer.new(
            Binding.mongocrypt_binary_new,
            Binding.method(:mongocrypt_binary_destroy)
          )
        end
      end

      # Initialize a Binary object from an existing pointer to a mongocrypt_binary_t
      # object.
      #
      # @param [ FFI::Pointer ] pointer A pointer to an existing
      #   mongocrypt_binary_t object
      #
      # @return [ Mongo::Crypt::Binary ] A new binary object
      def self.from_pointer(pointer)
        self.new(pointer: pointer)
      end

      # Initialize a Binary object with a string. The Binary object will store a
      # copy of the specified string and destroy the allocated memory when
      # it goes out of scope.
      #
      # @param [ String ] data A string to be wrapped by the Binary object
      #
      # @return [ Mongo::Crypt::Binary ] A new binary object
      def self.from_data(data)
        self.new(data: data)
      end

      # Overwrite the existing data wrapped by this Binary object
      #
      # @note The data passed in must not take up more memory than the
      # original memory allocated to the underlying mongocrypt_binary_t
      # object. Do NOT use this method unless required to do so by libmongocrypt.
      #
      # @param [ String ] data The new string data to be wrapped by this binary object
      #
      # @return [ true ] Always true
      #
      # @raise [ ArgumentError ] Raises when trying to write more data
      # than was originally allocated or when writing to an object that
      # already owns data.
      def write(data)
        if @data
          raise ArgumentError, 'Cannot write to an owned Binary'
        end

        # Cannot write a string that's longer than the space currently allocated
        # by the mongocrypt_binary_t object
        str_p = Binding.mongocrypt_binary_data(ref)
        len = Binding.mongocrypt_binary_len(ref)

        if len < data.bytesize
          raise ArgumentError.new(
            "Cannot write #{data.bytesize} bytes of data to a Binary object " +
            "that was initialized with #{Binding.mongocrypt_binary_len(@bin)} bytes."
          )
        end

        str_p.put_bytes(0, data)

        true
      end

      # Returns the data stored as a string
      #
      # @return [ String ] Data stored in the mongocrypt_binary_t as a string
      def to_s
        str_p = Binding.mongocrypt_binary_data(ref)
        len = Binding.mongocrypt_binary_len(ref)
        str_p.read_string(len)
      end

      # Returns the reference to the underlying mongocrypt_binary_t
      # object
      #
      # @return [ FFI::Pointer ] The underlying mongocrypt_binary_t object
      def ref
        @bin
      end

      # Wraps a String with a mongocrypt_binary_t, yielding an FFI::Pointer
      # to the wrapped struct.
      def self.wrap_string(str)
        binary_p = Binding.mongocrypt_binary_new_from_data(
          FFI::MemoryPointer.from_string(str),
          str.bytesize,
        )
        begin
          yield binary_p
        ensure
          Binding.mongocrypt_binary_destroy(binary_p)
        end
      end
    end
  end
end

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

    # A Ruby binding for the libmongocrypt C library
    #
    # @api private
    #
    # @since 2.12.0
    class Binding
      extend FFI::Library

      unless ENV['LIBMONGOCRYPT_PATH']
        raise "Cannot load Mongo::Libmongocrypt::Binding because there is no path " +
            "to libmongocrypt specified in the LIBMONGOCRYPT_PATH environment variable."
      end

      begin
        ffi_lib ENV['LIBMONGOCRYPT_PATH']
      rescue LoadError => e
        raise "Cannot load Mongo::Libmongocrypt::Binding because the path to " +
          "libmongocrypt specified in the LIBMONGOCRYPT_PATH environment variable " +
          "is invalid: #{ENV['LIBMONGOCRYPT']}\n\n#{e.class}: #{e.message}"
      end

      # Takes an integer pointer as an optional out parameter specifying
      # the return string length.
      # Returns the version string for the libmongocrypt library
      attach_function :mongocrypt_version, [:pointer], :string

      # Returns a pointer to a new mongocrypt_binary_t
      attach_function :mongocrypt_binary_new, [], :pointer

      # Takes a pointer to an array of uint-8 bytes and an integer length
      # Returns a pointer to a new mongocrypt_binary_t wrapping the specified byte buffer.
      attach_function :mongocrypt_binary_new_from_data, [:pointer, :int], :pointer

      # Takes a mongocrypt_binary_t pointer
      # Returns a pointer to the byte array wrapped by the mongocrypt_binary_t
      attach_function :mongocrypt_binary_data, [:pointer], :pointer

      # Takes a mongocrypt_binary_t pointer
      # Returns the length of the byte array wrapped by the mongocrypt_binary_t
      attach_function :mongocrypt_binary_len, [:pointer], :int

      # Takes a mongocrypt_binary_t pointer
      # Frees the reference to that mongocrypt_binary_t
      attach_function :mongocrypt_binary_destroy, [:pointer], :void
    end
  end
end

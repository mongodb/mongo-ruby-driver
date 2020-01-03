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

    # A Ruby binding for the libmongocrypt C library
    #
    # @api private
    class Binding
      extend FFI::Library

      unless ENV['LIBMONGOCRYPT_PATH']
        raise "Cannot load Mongo::Crypt::Binding because there is no path " +
            "to libmongocrypt specified in the LIBMONGOCRYPT_PATH environment variable."
      end

      begin
        ffi_lib ENV['LIBMONGOCRYPT_PATH']
      rescue LoadError => e
        raise "Cannot load Mongo::Crypt::Binding because the path to " +
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

      # Status types
      enum :status_type, [
        :ok,            0,
        :error_client,  1,
        :error_kms,     2,
      ]

      # Creates a new status object to retrieve from a mongocrypt_t handle
      # and returns the pointer to that status
      attach_function :mongocrypt_status_new, [], :pointer

      # Takes:
      # - a pointer to a status
      # - a status type (defined in :status_type enum)
      # - an integer error code
      # - a string error message
      # - an integer that is the length of the string + 1
      # Sets the status_type, error code, and error message on the specified status
      attach_function :mongocrypt_status_set, [:pointer, :status_type, :int, :string, :int], :void

      # Takes a pointer to a mongocrypt_status_t object and returns the status
      # type set on that object
      attach_function :mongocrypt_status_type, [:pointer], :status_type

      # Takes a pointer to a mongocrypt_status_t object and returns the status
      # code set on that object
      attach_function :mongocrypt_status_code, [:pointer], :int

      # Takes a pointer to a mongocrypt_status_t object and returns the status
      # message set on that object. Takes an optional out parameter specifying
      # the length of the returned string.
      attach_function :mongocrypt_status_message, [:pointer, :pointer], :string

      # Takes a pointer to a mongocrypt_status_t object and returns whether or not
      # the status type is ok
      attach_function :mongocrypt_status_ok, [:pointer], :bool

      # Takes a pointer to a mongocrypt_status_t object and destroys the
      # reference to that status
      attach_function :mongocrypt_status_destroy, [:pointer], :void

      # Log level
      enum :log_level, [
        :fatal,   0,
        :error,   1,
        :warn,    2,
        :info,    3,
        :debug,   4,
      ]

      # Mongocrypt log function signature. Takes a log level, a log message as a string,
      # an integer representing the length of the message, and a pointer to a context provided
      # by the caller (can be set to nil).
      callback :mongocrypt_log_fn_t, [:log_level, :string, :int, :pointer], :void

      # Sets a method to be called on every log message. Takes a pointer to a mongocrypt_t object,
      # a mongocrypt_log_fn_t callback, and a pointer to a log_ctx. Returns a boolean indicating
      # success of the operation.
      attach_function :mongocrypt_setopt_log_handler, [:pointer, :mongocrypt_log_fn_t, :pointer], :bool

      # Creates a new mongocrypt_t object and returns a pointer to that object
      attach_function :mongocrypt_new, [], :pointer

      # Takes a pointer to a mongocrypt_t object and a pointer to a mongocrypt_binary_t
      # object wrapping a 96-byte master key for encryption/decryption.
      # Configures the mongocrypt_t with the KMS provider options and returns a boolean
      # indicating the success of the operation.
      attach_function :mongocrypt_setopt_kms_provider_local, [:pointer, :pointer], :bool

      # Takes a pointer to a mongocrypt_t object and a pointer to a mongocrypt_binary_t
      # object. Sets the mongocrypt schema map to the string wrapped by the binary and
      # returns a boolean indicating the success of the operation.
      attach_function :mongocrypt_setopt_schema_map, [:pointer, :pointer], :bool

      # Takes a pointer to a mongocrypt_t object and initializes that object.
      # Should be called after mongocrypt_setopt_kms_provider_local and other methods that
      # set options on the mongocrypt_t object.
      # Returns a boolean indicating the success of the operation.
      attach_function :mongocrypt_init, [:pointer], :bool

      # Takes a pointer to a mongocrypt_t object and a pointer to a mongocrypt_status_t
      # object as an out parameter. Sets the status information of the mongocrypt_t
      # on the specified status object. Returns a boolean indicating the success of
      # the operation.
      attach_function :mongocrypt_status, [:pointer, :pointer], :bool

      # Takes a pointer to a mongocrypt_t object and destroys the reference to that object.
      attach_function :mongocrypt_destroy, [:pointer], :void

      # Takes a pointer to a mongocrypt_t object
      # Creates a new mongocrypt_ctx_t object and returns a pointer to it
      attach_function :mongocrypt_ctx_new, [:pointer], :pointer

      # Takes a pointer to a mongocrypt_ctx_t object and a pointer to a mongocrypt_status_t
      # object as an out parameter. Sets the status information of the mongocrypt_ctx_t
      # on the specified status object. Returns a boolean indicating the success of
      # the operation.
      attach_function :mongocrypt_ctx_status, [:pointer, :pointer], :bool

      # Takes a pointer to a mongocrypt_ctx_t object and configures it to accept
      # a local KMS master key
      # Returns a boolean indicating the success of the operation
      attach_function :mongocrypt_ctx_setopt_masterkey_local, [:pointer], :bool

      # Takes a pointer to a mongocrypt_ctx_t object and initializes the
      # state machine in order to create a data key
      # Returns a boolean indiating the success of the operation
      attach_function :mongocrypt_ctx_datakey_init, [:pointer], :bool

      # Takes a pointer to a mongocrypt_ctx_t object and a pointer to a
      # mongocrypt_binary_t object wrapping the id of the key that will be used
      # to encrypt the data. Returns a boolean indicating the success of the operation.
      attach_function :mongocrypt_ctx_setopt_key_id, [:pointer, :pointer], :bool

      # Takes a pionter to a mongocrypt_ctx_t object, a string indicating the algorithm
      # name (valid values are "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic" and
      # "AEAD_AES_256_CBC_HMAC_SHA_512-Random") and an integer indicating the length
      # of the string. Returns a boolean indicating success of the operation.
      attach_function :mongocrypt_ctx_setopt_algorithm, [:pointer, :string, :int], :bool

      # Takes a pointer to a mongocrypt_ctx_t object and a pointer to a mongocrypt_binary_t
      # object that wraps the value to be encrypted. Initializes the state machine in order
      # to encrypt the specified value. Returns a boolean indicating the success of the
      # operation.
      attach_function :mongocrypt_ctx_explicit_encrypt_init, [:pointer, :pointer], :bool

      # Takes a pointer to a mongocrypt_ctx_t object and a pointer to a mongocrypt_binary_t
      # object that wraps the value to be decrypted. Initializes the state machine for
      # explicit decryption. Returns a boolean indicating the success of the operation.
      attach_function :mongocrypt_ctx_explicit_decrypt_init, [:pointer, :pointer], :bool

      # Takes a pointer to a mongocrypt_ctx_t object, the string name of the database against which
      # the command is being run, the length of the database name as an integer, and a pointer
      # to a mongocrypt_binary_t object wrapping the command to be encrypted. Initializes
      # the mongocrypt_ctx_t object for auto-encryption and returns a boolean indicating the
      # success of the operation.
      attach_function :mongocrypt_ctx_encrypt_init, [:pointer, :string, :int, :pointer], :bool

      # Takes a pointer to a mongocrypt_ctx_t object and a pointer to a mongocrypt_binary_t object
      # that wraps the value to be decrypted. Initializes the mongocrypt_ctx_t object for
      # auto-decryption and returns a boolean indicating the success of the operation.
      attach_function :mongocrypt_ctx_decrypt_init, [:pointer, :pointer], :bool

      # Takes a pointer to a mongocrypt_ctx_t object and destroys
      # the reference to that object
      attach_function :mongocrypt_ctx_destroy, [:pointer], :void

      # mongocrypt_ctx_state_t type
      enum :mongocrypt_ctx_state, [
        :error,               0,
        :need_mongo_collinfo, 1,
        :need_mongo_markings, 2,
        :need_mongo_keys,     3,
        :need_kms,            4,
        :ready,               5,
        :done,                6,
      ]

      # Takes a pointer to a mongocrypt_ctx_t object and returns a state code
      attach_function :mongocrypt_ctx_state, [:pointer], :mongocrypt_ctx_state

      # Takes a pointer to a mognocrypt_ctx_t object and a pointer to a
      # mongocrypt_binary_t object as an out parameter. Writes a BSON document
      # to the provided binary pointer; the purpose of this BSON document varies
      # depending on the state of the state machine. Returns a boolean indicating success.
      #
      # This method is not currently unit tested.
      attach_function :mongocrypt_ctx_mongo_op, [:pointer, :pointer], :bool

      # Takes a pointer to a mongocrypt_ctx_t object and a pointer to a
      # mongocrypt_binary_t object wrapping a BSON document. The BSON document
      # should be the result of performing the necessary operation with the
      # output of mongocrypt_ctx_mongo_op. This method can be called multiple
      # times in a row. Returns a boolean indicating the success of the operation.
      #
      # This method is not currently unit tested.
      attach_function :mongocrypt_ctx_mongo_feed, [:pointer, :pointer], :bool

      # Takes a pointer to a mongocrypt_ctx_t object. Marks that the
      # mongocrypt_ctx_t object has finished accepting input from the
      # mongocrypt_ctx_mongo_feed method. Returns a boolean indicating success of
      # the operation.
      #
      # This method is not currently unit tested.
      attach_function :mongocrypt_ctx_mongo_done, [:pointer], :bool

      # Takes a pointer to a mongocrypt_ctx_t object and an out param,
      # which is a pointer to a mongocrypt_binary_t object, which will serve
      # as a wrapper around the final results of the state machine. The meaning
      # of these results depends on how the montocrypt_ctx_t object was initialized.
      # Returns a boolean indicating the success of the operation.
      #
      # This method is not currently unit tested.
      attach_function :mongocrypt_ctx_finalize, [:pointer, :pointer], :void

      # A callback to a crypto AES-256-CBC encrypt/decrypt function. Takes:
      # - An optional pointer to a mongocrypt_ctx_t object
      # - A pointer to a mongocrypt_binary_t object that wraps a 32-byte encryption key
      # - A pointer to a mongocrypt_binary_t object that wraps a 16-byte iv
      # - A pointer to a mongocrypt_binary_t object that wraps the encryption/decryption input
      # - A pointer to a mongocrypt_binary_t object to which the encryption/decryption output will be written
      # - A pointer to an int32 where the number of bytes of the output will be written
      # - An optional pointer to a mongocrypt_status_t object for error messages
      # Returns a boolean indicating the success of the operation
      callback :mongocrypt_crypto_fn, [:pointer, :pointer, :pointer, :pointer, :pointer, :pointer, :pointer], :bool

      # A callback to a crypto HMAC SHA-512 or SHA-256 function. Takes:
      # - An optional pointer to a mongocrypt_ctx_t object
      # - A pointer to a mongocrypt_binary_t object that wraps a 32-byte encryption key
      # - A pointer to a mongocrypt_binary_t object that wraps the encryption input
      # - A pointer to a mongocrypt_binary_t object to which the output will be written
      # - An optional pointer to a mongocrypt_status_t object for error messages
      # Returns a boolean indicating the success of the operation
      callback :mongocrypt_hmac_fn, [:pointer, :pointer, :pointer, :pointer, :pointer], :bool


            # Mongocrypt log function signature. Takes a log level, a log message as a string,
      # an integer representing the length of the message, and a pointer to a context provided
      # by the caller (can be set to nil).
      # callback :mongocrypt_log_fn_t, [:log_level, :string, :int, :pointer], :void

      # # Sets a method to be called on every log message. Takes a pointer to a mongocrypt_t object,
      # # a mongocrypt_log_fn_t callback, and a pointer to a log_ctx. Returns a boolean indicating
      # # success of the operation.
      # attach_function :mongocrypt_setopt_log_handler, [:pointer, :mongocrypt_log_fn_t, :pointer], :boo
    end
  end
end

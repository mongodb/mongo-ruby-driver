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

unless ENV['LIBMONGOCRYPT_PATH']
  # It seems that MRI maintains autoload configuration for a module until
  # that module is defined, but JRuby removes autoload configuration as soon
  # as the referenced file is attempted to be loaded, even if the module
  # never ends up being defined.
  if BSON::Environment.jruby?
    module Mongo
      module Crypt
        autoload :Binding, 'mongo/crypt/binding'
      end
    end
  end

  raise LoadError, "Cannot load Mongo::Crypt::Binding because there is no path " +
      "to libmongocrypt specified in the LIBMONGOCRYPT_PATH environment variable."
end

require 'ffi'

module Mongo
  module Crypt

    # @api private
    def reset_autoload
      remove_const(:Binding)
      autoload(:Binding, 'mongo/crypt/binding')
    end
    module_function :reset_autoload

    # A Ruby binding for the libmongocrypt C library
    #
    # @api private
    class Binding
      extend FFI::Library

      begin
        ffi_lib ENV['LIBMONGOCRYPT_PATH']
      rescue LoadError => e
        Crypt.reset_autoload
        raise LoadError, "Cannot load Mongo::Crypt::Binding because the path to " +
          "libmongocrypt specified in the LIBMONGOCRYPT_PATH environment variable " +
          "is invalid: #{ENV['LIBMONGOCRYPT_PATH']}\n\n#{e.class}: #{e.message}"
      end

      # Returns the version string of the libmongocrypt library
      #
      # @param [ FFI::Pointer | nil ] len (out param) An optional pointer to a
      #   uint8 that will reference the length of the returned string.
      #
      # @return [ String ] A version string for libmongocrypt
      attach_function :mongocrypt_version, [:pointer], :string

      # Create a new mongocrypt_binary_t object (a non-owning view of a byte
      # array)
      #
      # @return [ FFI::Pointer ] A pointer to the newly-created
      #   mongocrypt_binary_t object
      attach_function :mongocrypt_binary_new, [], :pointer

      # Create a new mongocrypt_binary_t object that maintains a pointer to
      # the specified byte array.
      #
      # @param [ FFI::Pointer ] data A pointer to an array of bytes; the data
      #   is not copied and must outlive the mongocrypt_binary_t object
      # @param [ Integer ] len The length of the array argument
      #
      # @return [ FFI::Pointer ] A pointer to the newly-created
      #   mongocrypt_binary_t object
      attach_function(
        :mongocrypt_binary_new_from_data,
        [:pointer, :int],
        :pointer
      )

      # Get the pointer to the underlying data for the mongocrypt_binary_t
      #
      # @param [ FFI::Pointer ] binary A pointer to a mongocrypt_binary_t object
      #
      # @return [ FFI::Pointer ] A pointer to the data array
      attach_function :mongocrypt_binary_data, [:pointer], :pointer

      # Get the length of the underlying data array
      #
      # @param [ FFI::Pointer ] binary A pointer to a mongocrypt_binary_t object
      #
      # @return [ Integer ] The length of the data array
      attach_function :mongocrypt_binary_len, [:pointer], :int

      # Destroy the mongocrypt_binary_t object
      #
      # @param [ FFI::Pointer ] A pointer to a mongocrypt_binary_t object
      #
      # @return [ nil ] Always nil
      attach_function :mongocrypt_binary_destroy, [:pointer], :void

      # Enum labeling different status types
      enum :status_type, [
        :ok,            0,
        :error_client,  1,
        :error_kms,     2,
      ]

      # Create a new mongocrypt_status_t object
      #
      # @return [ FFI::Pointer ] A pointer to the new mongocrypt_status_ts
      attach_function :mongocrypt_status_new, [], :pointer

      # Set a message, type, and code on an existing status
      #
      # @param [ FFI::Pointer ] status A pointer to a mongocrypt_status_t
      # @param [ Symbol ] type The status type; possible values are defined
      #   by the status_type enum
      # @param [ Integer ] code The status code
      # @param [ String ] message The status message
      # @param [ Integer ] len The length of the message argument (or -1 for a
      #   null-terminated string)
      #
      # @return [ nil ] Always nil
      attach_function(
        :mongocrypt_status_set,
        [:pointer, :status_type, :int, :string, :int],
        :void
      )

      # Indicates the status type
      #
      # @param [ FFI::Pointer ] status A pointer to a mongocrypt_status_t
      #
      # @return [ Symbol ] The status type (as defined by the status_type enum)
      attach_function :mongocrypt_status_type, [:pointer], :status_type

      # Return the status error code
      #
      # @param [ FFI::Pointer ] status A pointer to a mongocrypt_status_t
      #
      # @return [ Integer ] The status code
      attach_function :mongocrypt_status_code, [:pointer], :int

      # Returns the status message
      #
      # @param [ FFI::Pointer ] status A pointer to a mongocrypt_status_t
      # @param [ FFI::Pointer | nil ] len (out param) An optional pointer to a
      #   uint32, where the length of the retun string will be written
      #
      # @return [ String ] The status message
      attach_function :mongocrypt_status_message, [:pointer, :pointer], :string

      # Returns whether the status is ok or an error
      #
      # @param [ FFI::Pointer ] status A pointer to a mongocrypt_status_t
      #
      # @return [ Boolean ] Whether the status is ok
      attach_function :mongocrypt_status_ok, [:pointer], :bool

      # Destroys the reference to the mongocrypt_status_t object
      #
      # @param [ FFI::Pointer ] status A pointer to a mongocrypt_status_t
      #
      # @return [ nil ] Always nil
      attach_function :mongocrypt_status_destroy, [:pointer], :void

      # Enum labeling the various log levels
      enum :log_level, [
        :fatal,   0,
        :error,   1,
        :warn,    2,
        :info,    3,
        :debug,   4,
      ]

      # A callback to the mongocrypt log function
      # Set a custom log callback with the mongocrypt_setopt_log_handler method
      #
      # @param [ Symbol ] level The log level; possible values defined by the
      #   log_level enum
      # @param [ String ] message The log message
      # @param [ Integer ] len The length of the message param, or -1 if the
      #   string is null terminated
      # @param [ FFI::Pointer | nil ] ctx An optional pointer to a context
      #   object when this callback was set
      #
      # @return [ nil ] Always nil.
      callback :mongocrypt_log_fn_t, [:log_level, :string, :int, :pointer], :void

      # Creates a new mongocrypt_t object
      #
      # @return [ FFI::Pointer ] A pointer to a new mongocrypt_t object
      attach_function :mongocrypt_new, [], :pointer

      # Set the handler on the mongocrypt_t object to be called every time
      #   libmongocrypt logs a message
      #
      # @param [ FFI::Pointer ] crypt A pointer to a mongocrypt_t object
      # @param [ Method ] log_fn A logging callback method
      # @param [ FFI::Pointer | nil ] log_ctx An optional pointer to a context
      #   to be passed into the log callback on every invocation.
      #
      # @return [ Boolean ] Whether setting the callback was successful
      attach_function(
        :mongocrypt_setopt_log_handler,
        [:pointer, :mongocrypt_log_fn_t, :pointer],
        :bool
      )

      def setopt_log_handler(handle, log_callback)
        check_status(handle) do
          mongocrypt_setopt_log_handler(handle, log_callback, nil)
        end
      end

      # Configure mongocrypt_t object to take local KSM provider options
      #
      # @param [ FFI::Pointer ] crypt A pointer to a mongocrypt_t object
      # @param [ FFI::Pointer ] key A pointer to a mongocrypt_binary_t object
      #   that references the 96-byte local master key
      #
      # @return [ Boolean ] Returns whether the option was set successfully
      attach_function(
        :mongocrypt_setopt_kms_provider_local,
        [:pointer, :pointer],
        :bool
      )

      def self.setopt_kms_provider_local(handle, raw_master_key)
        Binary.wrap_string(raw_master_key) do |master_key_p|
          check_status(handle) do
            mongocrypt_setopt_kms_provider_local(handle.ref, master_key_p)
          end
        end
      end

      # Sets a local schema map for encryption
      #
      # @param [ FFI::Pointer ] crypt A pointer to a mongocrypt_t object
      # @param [ FFI::Pointer ] schema_map A pointer to a mongocrypt_binary_t
      #   object that references the schema map as a BSON binary string
      #
      # @return [ Boolean ] Returns whether the option was set successfully
      attach_function :mongocrypt_setopt_schema_map, [:pointer, :pointer], :bool

      def self.setopt_schema_map(handle, schema_map_doc)
        data = schema_map_doc.to_bson.to_s
        Binary.wrap_string(data) do |data_p|
          check_status(handle) do
            mongocrypt_setopt_schema_map(handle.ref, data_p)
          end
        end
      end

      # Initialize the mongocrypt_t object
      #
      # @param [ FFI::Pointer ] crypt A pointer to a mongocrypt_t object
      #
      # @return [ Boolean ] Returns whether the crypt was initialized successfully
      attach_function :mongocrypt_init, [:pointer], :bool

      def self.init(handle)
        check_status(handle) do
          mongocrypt_init(handle.ref)
        end
      end

      # Set the status information from the mongocrypt_t object on the
      # mongocrypt_status_t object
      #
      # @param [ FFI::Pointer ] crypt A pointer to a mongocrypt_t object
      # @param [ FFI::Pointer ] status A pointer to a mongocrypt_status_t object
      #
      # @return [ Boolean ] Whether the status was successfully set
      attach_function :mongocrypt_status, [:pointer, :pointer], :bool

      # Destroy the reference the mongocrypt_t object
      #
      # @param [ FFI::Pointer ] crypt A pointer to a mongocrypt_t object
      #
      # @return [ nil ] Always nil
      attach_function :mongocrypt_destroy, [:pointer], :void

      # Create a new mongocrypt_ctx_t object (a wrapper for the libmongocrypt
      #   state machine)
      #
      # @param [ FFI::Pointer ] crypt A pointer to a mongocrypt_t object
      #
      # @return [ FFI::Pointer ] A new mongocrypt_ctx_t object
      attach_function :mongocrypt_ctx_new, [:pointer], :pointer

      # Set the status information from the mongocrypt_ctx_t object on the
      # mongocrypt_status_t object
      #
      # @param [ FFI::Pointer ] ctx A pointer to a mongocrypt_ctx_t object
      # @param [ FFI::Pointer ] status A pointer to a mongocrypt_status_t object
      #
      # @return [ Boolean ] Whether the status was successfully set
      attach_function :mongocrypt_ctx_status, [:pointer, :pointer], :bool

      # Set the key id used for explicit encryption
      #
      # @param [ FFI::Pointer ] ctx A pointer to a mongocrypt_ctx_t object
      # @param [ FFI::Pointer ] key_id A pointer to a mongocrypt_binary_t object
      #   that references the 16-byte key-id
      #
      # @note Do not initialize ctx before calling this method
      # @return [ Boolean ] Whether the option was successfully set
      attach_function :mongocrypt_ctx_setopt_key_id, [:pointer, :pointer], :bool

      # Sets the key id option on an explicit encryption context.
      #
      # @param [ Context ] context Explicit encryption context
      # @param [ String ] key_id The key id
      #
      # @raise [ Error::CryptError ] If the operation failed
      def self.ctx_setopt_key_id(context, key_id)
        Binary.wrap_string(key_id) do |key_id_p|
          check_ctx_status(context) do
            mongocrypt_ctx_setopt_key_id(context.ctx_p, key_id_p)
          end
        end
      end

      # Set the algorithm used for explicit encryption
      #
      # @param [ FFI::Pointer ] ctx A pointer to a mongocrypt_ctx_t object
      # @param [ String ] algorithm The algorithm name. Valid values are:
      #   - "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic"
      #   - "AEAD_AES_256_CBC_HMAC_SHA_512-Random"
      # @param [ Integer ] len The length of the algorithm string
      #
      # @note Do not initialize ctx before calling this method
      # @return [ Boolean ] Whether the option was successfully set
      attach_function(
        :mongocrypt_ctx_setopt_algorithm,
        [:pointer, :string, :int],
        :bool
      )

      def self.ctx_setopt_algorithm(context, name)
        check_ctx_status(context) do
          mongocrypt_ctx_setopt_algorithm(context.ctx_p, name, -1)
        end
      end

      # Set the ctx to take a local master key
      #
      # @param [ FFI::Pointer ] ctx A pointer to a mongocrypt_ctx_t object
      #
      # @note Do not initialize ctx before calling this method
      # @return [ Boolean ] Whether the option was successfully set
      attach_function(
        :mongocrypt_ctx_setopt_masterkey_local,
        [:pointer],
        :bool
      )

      def self.ctx_setopt_masterkey_local(context)
        check_ctx_status(context) do
          mongocrypt_ctx_setopt_masterkey_local(context.ctx_p)
        end
      end

      # Initializes the ctx to create a data key
      #
      # @param [ FFI::Pointer ] ctx A pointer to a mongocrypt_ctx_t object
      #
      # @note Before calling this method, masterkey options must be set.
      #   Set AWS masterkey by calling mongocrypt_ctx_setopt_masterkey_aws
      #   and mongocrypt_ctx_setopt_masterkey_aws_endpoint. Set local master
      #   key by calling mongocrypt_ctx_setopt_masterkey_local.
      #
      # @return [ Boolean ] Whether the initialization was successful
      attach_function :mongocrypt_ctx_datakey_init, [:pointer], :bool

      def self.ctx_datakey_init(context)
        check_ctx_status(context) do
          mongocrypt_ctx_datakey_init(context.ctx_p)
        end
      end

      # Initializes the ctx for auto-encryption
      #
      # @param [ FFI::Pointer ] ctx A pointer to a mongocrypt_ctx_t object
      # @param [ String ] db The database name
      # @param [ Integer ] db_len The length of the database name argument (or
      #   -1 for a null-terminated string)
      # @param [ FFI::Pointer ] cmd A pointer to a mongocrypt_binary_t object
      #   that references the database command as a binary string
      #
      # @note This method expects the passed-in BSON to be in the format:
      #   { "v": BSON value to decrypt }
      #
      # @return [ Boolean ] Whether the initialization was successful
      attach_function(
        :mongocrypt_ctx_encrypt_init,
        [:pointer, :string, :int, :pointer],
        :bool
      )

      def self.ctx_encrypt_init(context, db_name, an_int, command)
        data = command.to_bson.to_s
        Binary.wrap_string(data) do |data_p|
          check_ctx_status(context) do
            mongocrypt_ctx_encrypt_init(context.ctx_p, db_name, an_int, data_p)
          end
        end
      end

      # Initializes the ctx for explicit encryption
      #
      # @param [ FFI::Pointer ] ctx A pointer to a mongocrypt_ctx_t object
      # @param [ FFI::Pointer ] msg A pointer to a mongocrypt_binary_t object
      #   that references the message to be encrypted as a binary string
      #
      # @note Before calling this method, set a key_id, key_alt_name (optional),
      #   and encryption algorithm using the following methods:
      #   mongocrypt_ctx_setopt_key_id, mongocrypt_ctx_setopt_key_alt_name,
      #   and mongocrypt_ctx_setopt_algorithm
      #
      # @return [ Boolean ] Whether the initialization was successful
      attach_function(
        :mongocrypt_ctx_explicit_encrypt_init,
        [:pointer, :pointer],
        :bool
      )

      def self.ctx_explicit_encrypt_init(context, doc)
        data = doc.to_bson.to_s
        Binary.wrap_string(data) do |data_p|
          check_ctx_status(context) do
            mongocrypt_ctx_explicit_encrypt_init(context.ctx_p, data_p)
          end
        end
      end

      # Initializes the ctx for auto-decryption
      #
      # @param [ FFI::Pointer ] ctx A pointer to a mongocrypt_ctx_t object
      # @param [ FFI::Pointer ] doc A pointer to a mongocrypt_binary_t object
      #   that references the document to be decrypted as a BSON binary string
      #
      # @return [ Boolean ] Whether the initialization was successful
      attach_function :mongocrypt_ctx_decrypt_init, [:pointer, :pointer], :bool

      def self.ctx_decrypt_init(context, command)
        data = command.to_bson.to_s
        Binary.wrap_string(data) do |data_p|
          check_ctx_status(context) do
            mongocrypt_ctx_decrypt_init(context.ctx_p, data_p)
          end
        end
      end

      # Initializes the ctx for explicit decryption
      #
      # @param [ FFI::Pointer ] ctx A pointer to a mongocrypt_ctx_t object
      # @param [ FFI::Pointer ] msg A pointer to a mongocrypt_binary_t object
      #   that references the message to be decrypted as a BSON binary string
      #
      # @return [ Boolean ] Whether the initialization was successful
      attach_function(
        :mongocrypt_ctx_explicit_decrypt_init,
        [:pointer, :pointer],
        :bool
      )

      def self.ctx_explicit_decrypt_init(context, doc)
        data = doc.to_bson.to_s
        Binary.wrap_string(data) do |data_p|
          check_ctx_status(context) do
            mongocrypt_ctx_explicit_decrypt_init(context.ctx_p, data_p)
          end
        end
      end

      # An enum labeling different libmognocrypt state machine states
      enum :mongocrypt_ctx_state, [
        :error,               0,
        :need_mongo_collinfo, 1,
        :need_mongo_markings, 2,
        :need_mongo_keys,     3,
        :need_kms,            4,
        :ready,               5,
        :done,                6,
      ]

      # Get the current state of the ctx
      #
      # @param [ FFI::Pointer ] ctx A pointer to a mongocrypt_ctx_t object
      #
      # @return [ Symbol ] The current state, will be one of the values defined
      #   by the mongocrypt_ctx_state enum
      attach_function :mongocrypt_ctx_state, [:pointer], :mongocrypt_ctx_state

      # Get a BSON operation for the driver to run against the MongoDB
      # collection, the key vault database, or mongocryptd.
      #
      # @param [ FFI::Pointer ] ctx A pointer to a mongocrypt_ctx_t object
      # @param [ FFI::Pointer ] op_bson (out param) A pointer to a
      #   mongocrypt_binary_t object that will have a reference to the
      #   BSON operation written to it by libmongocrypt
      #
      # @return [ Boolean ] A boolean indicating the success of the operation
      attach_function :mongocrypt_ctx_mongo_op, [:pointer, :pointer], :bool

      # Returns a BSON::Document representing an operation that the
      # driver must perform on behalf of libmongocrypt to get the
      # information it needs in order to continue with
      # encryption/decryption (for example, a filter for a key vault query).
      def self.ctx_mongo_op(context)
        binary = Binary.new

        check_ctx_status(context) do
          mongocrypt_ctx_mongo_op(context.ctx_p, binary.ref)
        end

        # TODO since the binary references a C pointer, and ByteBuffer is
        # written in C in MRI, we could omit a copy of the data by making
        # ByteBuffer reference the string that is owned by libmongocrypt.
        BSON::Document.from_bson(BSON::ByteBuffer.new(binary.to_string))
      end

      # Feed a BSON reply to libmongocrypt
      #
      # @param [ FFI::Pointer ] ctx A pointer to a mongocrypt_ctx_t object
      # @param [ FFI::Pointer ] reply A mongocrypt_binary_t object that
      #   references the BSON reply to feed to libmongocrypt
      #
      # @return [ Boolean ] A boolean indicating the success of the operation
      attach_function :mongocrypt_ctx_mongo_feed, [:pointer, :pointer], :bool

      def self.ctx_mongo_feed(context, doc)
        data = doc.to_bson.to_s
        Binary.wrap_string(data) do |data_p|
          check_ctx_status(context) do
            mongocrypt_ctx_mongo_feed(context.ctx_p, data_p)
          end
        end
      end

      # Indicate to libmongocrypt that the driver is done feeding replies
      #
      # @param [ FFI::Pointer ] ctx A pointer to a mongocrypt_ctx_t object
      #
      # @return [ Boolean ] A boolean indicating the success of the operation
      attach_function :mongocrypt_ctx_mongo_done, [:pointer], :bool

      # Perform the final encryption or decryption and return a BSON document
      #
      # @param [ FFI::Pointer ] ctx A pointer to a mongocrypt_ctx_t object
      # @param [ FFI::Pointer ] op_bson (out param) A pointer to a
      #   mongocrypt_binary_t object that will have a reference to the
      #   final encrypted BSON document
      #
      # @return [ Boolean ] A boolean indicating the success of the operation
      attach_function :mongocrypt_ctx_finalize, [:pointer, :pointer], :void

      def self.ctx_finalize(context)
        binary = Binary.new

        check_ctx_status(context) do
          mongocrypt_ctx_finalize(context.ctx_p, binary.ref)
        end

        # TODO since the binary references a C pointer, and ByteBuffer is
        # written in C in MRI, we could omit a copy of the data by making
        # ByteBuffer reference the string that is owned by libmongocrypt.
        BSON::Document.from_bson(BSON::ByteBuffer.new(binary.to_string))
      end

      # Destroy the reference to the mongocrypt_ctx_t object
      #
      # @param [ FFI::Pointer ] ctx A pointer to a mongocrypt_ctx_t object
      #
      # @return [ nil ] Always nil
      attach_function :mongocrypt_ctx_destroy, [:pointer], :void

      # A callback to a function that performs AES encryption or decryption
      #
      # @param [ FFI::Pointer | nil] ctx An optional pointer to a context object
      #   that may have been set when hooks were enabled.
      # @param [ FFI::Pointer ] key A pointer to a mongocrypt_binary_t object
      #   that references the 32-byte AES encryption key
      # @param [ FFI::Pointer ] iv A pointer to a mongocrypt_binary_t object
      #   that references the 16-byte AES IV
      # @param [ FFI::Pointer ] in A pointer to a mongocrypt_binary_t object
      #   that references the value to be encrypted/decrypted
      # @param [ FFI::Pointer ] out (out param) A pointer to a
      #   mongocrypt_binary_t object will have a reference to the encrypted/
      #   decrypted value written to it by libmongocrypt
      # @param [ FFI::Pointer ] status A pointer to a mongocrypt_status_t
      #   object to which an error message will be written if encryption fails
      #
      # @return [ Bool ] Whether encryption/decryption was successful
      callback(
        :mongocrypt_crypto_fn,
        [:pointer, :pointer, :pointer, :pointer, :pointer, :pointer, :pointer],
        :bool
      )

      # A callback to a function that performs HMAC SHA-512 or SHA-256
      #
      # @param [ FFI::Pointer | nil ] ctx An optional pointer to a context object
      #   that may have been set when hooks were enabled.
      # @param [ FFI::Pointer ] key A pointer to a mongocrypt_binary_t object
      #   that references the 32-byte HMAC SHA encryption key
      # @param [ FFI::Pointer ] in A pointer to a mongocrypt_binary_t object
      #   that references the input value
      # @param [ FFI::Pointer ] out (out param) A pointer to a
      #   mongocrypt_binary_t object will have a reference to the output value
      #   written to it by libmongocrypt
      # @param [ FFI::Pointer ] status A pointer to a mongocrypt_status_t
      #   object to which an error message will be written if encryption fails
      #
      # @return [ Bool ] Whether HMAC-SHA was successful
      callback(
        :mongocrypt_hmac_fn,
        [:pointer, :pointer, :pointer, :pointer, :pointer],
        :bool
      )

      # A callback to a SHA-256 hash function
      #
      # @param [ FFI::Pointer | nil ] ctx An optional pointer to a context object
      #   that may have been set when hooks were enabled.
      # @param [ FFI::Pointer ] in A pointer to a mongocrypt_binary_t object
      #   that references the value to be hashed
      # @param [ FFI::Pointer ] out (out param) A pointer to a
      #   mongocrypt_binary_t object will have a reference to the output value
      #   written to it by libmongocrypt
      # @param [ FFI::Pointer ] status A pointer to a mongocrypt_status_t
      #   object to which an error message will be written if encryption fails
      #
      # @return [ Bool ] Whether hashing was successful
      callback :mongocrypt_hash_fn, [:pointer, :pointer, :pointer, :pointer], :bool

      # A callback to a crypto secure random function
      #
      # @param [ FFI::Pointer | nil ] ctx An optional pointer to a context object
      #   that may have been set when hooks were enabled.
      # @param [ FFI::Pointer ] out (out param) A pointer to a
      #   mongocrypt_binary_t object will have a reference to the output value
      #   written to it by libmongocrypt
      # @param [ Integer ] count The number of random bytes to return
      # @param [ FFI::Pointer ] status A pointer to a mongocrypt_status_t
      #   object to which an error message will be written if encryption fails
      #
      # @return [ Bool ] Whether hashing was successful
      callback :mongocrypt_random_fn, [:pointer, :pointer, :int, :pointer], :bool

      # Set crypto hooks on the provided mongocrypt object
      #
      # @param [ FFI::Pointer ] crypt A pointer to a mongocrypt_t object
      # @param [ Method ] An AES encryption method
      # @param [ Method ] An AES decryption method
      # @param [ Method ] A random method
      # @param [ Method ] A HMAC-SHA-512 method
      # @param [ Method ] A HMAC-SHA-256 method
      # @param [ Method ] A SHA-256 hash method
      # @param [ FFI::Pointer | nil ] ctx An optional pointer to a context object
      #   that may have been set when hooks were enabled.
      #
      # @return [ Boolean ] Whether setting this option succeeded
      attach_function(
        :mongocrypt_setopt_crypto_hooks,
        [
          :pointer,
          :mongocrypt_crypto_fn,
          :mongocrypt_crypto_fn,
          :mongocrypt_random_fn,
          :mongocrypt_hmac_fn,
          :mongocrypt_hmac_fn,
          :mongocrypt_hash_fn,
          :pointer
        ],
        :bool
      )

      def self.setopt_crypto_hooks(handle,
        aes_encrypt_cb, aes_decrypt_cb, random_cb,
        hmac_sha_512_cb, hmac_sha_256_cb, hmac_hash_cb
      )
        check_status(handle) do
          mongocrypt_setopt_crypto_hooks(handle.ref,
            aes_encrypt_cb, aes_decrypt_cb, random_cb,
            hmac_sha_512_cb, hmac_sha_256_cb, hmac_hash_cb, nil
          )
        end
      end

      # Raise a Mongo::Error::CryptError based on the status of the underlying
      # mongocrypt_t object.
      #
      # @return [ nil ] Always nil.
      def self.check_status(handle)
        unless yield
          status = Status.new

          mongocrypt_status(handle.ref, status.ref)
          status.raise_crypt_error
        end
      end

      # Raise a Mongo::Error::CryptError based on the status of the underlying
      # mongocrypt_ctx_t object.
      #
      # @return [ nil ] Always nil.
      def self.check_ctx_status(context)
        if block_given?
          do_raise = !yield
        else
          do_raise = true
        end

        if do_raise
          status = Status.new

          mongocrypt_ctx_status(context.ctx_p, status.ref)
          status.raise_crypt_error
        end
      end
    end
  end
end

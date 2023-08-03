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

unless ENV['LIBMONGOCRYPT_PATH']
  begin
    require 'libmongocrypt_helper'
  rescue LoadError => e
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

    # JRuby 9.3.2.0 replaces a LoadError with our custom message with a
    # generic NameError, when this load is attempted as part of autoloading
    # process. JRuby 9.2.20.0 propagates LoadError as expected.
    raise LoadError, "Cannot load Mongo::Crypt::Binding because there is no path " +
        "to libmongocrypt specified in the LIBMONGOCRYPT_PATH environment variable " +
        "and libmongocrypt-helper is not installed: #{e.class}: #{e}"
  end
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

      if ENV['LIBMONGOCRYPT_PATH']
        begin
          ffi_lib ENV['LIBMONGOCRYPT_PATH']
        rescue LoadError => e
          Crypt.reset_autoload
          raise LoadError, "Cannot load Mongo::Crypt::Binding because the path to " +
            "libmongocrypt specified in the LIBMONGOCRYPT_PATH environment variable " +
            "is invalid: #{ENV['LIBMONGOCRYPT_PATH']}\n\n#{e.class}: #{e.message}"
        end
      else
        begin
          ffi_lib LibmongocryptHelper.libmongocrypt_path
        rescue LoadError => e
          Crypt.reset_autoload
          raise LoadError, "Cannot load Mongo::Crypt::Binding because the path to " +
            "libmongocrypt specified in libmongocrypt-helper " +
            "is invalid: #{LibmongocryptHelper.libmongocrypt_path}\n\n#{e.class}: #{e.message}"
        end
      end

      # Minimum version of libmongocrypt required by this version of the driver.
      # An attempt to use the driver with any previous version of libmongocrypt
      # will cause a `LoadError`.
      #
      # @api private
      MIN_LIBMONGOCRYPT_VERSION = Gem::Version.new("1.7.0")

      # @!method self.mongocrypt_version(len)
      #   @api private
      #
      #   Returns the version string of the libmongocrypt library.
      #   @param [ FFI::Pointer | nil ] len (out param) An optional pointer to a
      #     uint8 that will reference the length of the returned string.
      #   @return [ String ] A version string for libmongocrypt.
      attach_function :mongocrypt_version, [:pointer], :string

      # Given a string representing a version number, parses it into a
      # Gem::Version object. This handles the case where the string is not
      # in a format supported by Gem::Version by doing some custom parsing.
      #
      # @param [ String ] version String representing a version number.
      #
      # @return [ Gem::Version ] the version number
      #
      # @raise [ ArgumentError ] if the string cannot be parsed.
      #
      # @api private
      def self.parse_version(version)
        Gem::Version.new(version)
      rescue ArgumentError
        match = version.match(/\A(?<major>\d+)\.(?<minor>\d+)\.(?<patch>\d+)?(-[A-Za-z\+\d]+)?\z/)
        raise ArgumentError.new("Malformed version number string #{version}") if match.nil?

        Gem::Version.new(
          [
            match[:major],
            match[:minor],
            match[:patch]
          ].join('.')
        )
      end

      # Validates if provided version of libmongocrypt is valid, i.e. equal or
      # greater than minimum required version. Raises a LoadError if not.
      #
      # @param [ String ] lmc_version String representing libmongocrypt version.
      #
      # @raise [ LoadError ] if given version is lesser than minimum required version.
      #
      # @api private
      def self.validate_version(lmc_version)
        if (actual_version = parse_version(lmc_version)) < MIN_LIBMONGOCRYPT_VERSION
          raise LoadError, "libmongocrypt version #{MIN_LIBMONGOCRYPT_VERSION} or above is required, " +
            "but version #{actual_version} was found."
        end
      end

      validate_version(mongocrypt_version(nil))

      # @!method self.mongocrypt_binary_new
      #   @api private
      #
      #   Creates a new mongocrypt_binary_t object (a non-owning view of a byte
      #     array).
      #   @return [ FFI::Pointer ] A pointer to the newly-created
      #     mongocrypt_binary_t object.
      attach_function :mongocrypt_binary_new, [], :pointer

      # @!method self.mongocrypt_binary_new_from_data(data, len)
      #   @api private
      #
      #   Create a new mongocrypt_binary_t object that maintains a pointer to
      #     the specified byte array.
      #   @param [ FFI::Pointer ] data A pointer to an array of bytes; the data
      #     is not copied and must outlive the mongocrypt_binary_t object.
      #   @param [ Integer ] len The length of the array argument.
      #   @return [ FFI::Pointer ] A pointer to the newly-created
      #     mongocrypt_binary_t object.
      attach_function(
        :mongocrypt_binary_new_from_data,
        [:pointer, :int],
        :pointer
      )

      # @!method self.mongocrypt_binary_data(binary)
      #   @api private
      #
      #   Get the pointer to the underlying data for the mongocrypt_binary_t.
      #   @param [ FFI::Pointer ] binary A pointer to a mongocrypt_binary_t object.
      #   @return [ FFI::Pointer ] A pointer to the data array.
      attach_function :mongocrypt_binary_data, [:pointer], :pointer

      # @!method self.mongocrypt_binary_len(binary)
      #   @api private
      #
      #   Get the length of the underlying data array.
      #   @param [ FFI::Pointer ] binary A pointer to a mongocrypt_binary_t object.
      #   @return [ Integer ] The length of the data array.
      attach_function :mongocrypt_binary_len, [:pointer], :int

      # @!method self.mongocrypt_binary_destroy(binary)
      #   @api private
      #
      #   Destroy the mongocrypt_binary_t object.
      #   @param [ FFI::Pointer ] binary A pointer to a mongocrypt_binary_t object.
      #   @return [ nil ] Always nil.
      attach_function :mongocrypt_binary_destroy, [:pointer], :void

      # Enum labeling different status types
      enum :status_type, [
        :ok,            0,
        :error_client,  1,
        :error_kms,     2,
      ]

      # @!method self.mongocrypt_status_new
      #   @api private
      #
      #   Create a new mongocrypt_status_t object.
      #   @return [ FFI::Pointer ] A pointer to the new mongocrypt_status_ts.
      attach_function :mongocrypt_status_new, [], :pointer

      # @!method self.mongocrypt_status_set(status, type, code, message, len)
      #   @api private
      #
      #   Set a message, type, and code on an existing status.
      #   @param [ FFI::Pointer ] status A pointer to a mongocrypt_status_t.
      #   @param [ Symbol ] type The status type; possible values are defined
      #     by the status_type enum.
      #   @param [ Integer ] code The status code.
      #   @param [ String ] message The status message.
      #   @param [ Integer ] len The length of the message argument (or -1 for a
      #     null-terminated string).
      #   @return [ nil ] Always nil.
      attach_function(
        :mongocrypt_status_set,
        [:pointer, :status_type, :int, :string, :int],
        :void
      )

      # @!method self.mongocrypt_status_type(status)
      #   @api private
      #
      #   Indicates the status type.
      #   @param [ FFI::Pointer ] status A pointer to a mongocrypt_status_t.
      #   @return [ Symbol ] The status type (as defined by the status_type enum).
      attach_function :mongocrypt_status_type, [:pointer], :status_type

      # @!method self.mongocrypt_status_code(status)
      #   @api private
      #
      #   Return the status error code.
      #   @param [ FFI::Pointer ] status A pointer to a mongocrypt_status_t.
      #   @return [ Integer ] The status code.
      attach_function :mongocrypt_status_code, [:pointer], :int

      # @!method self.mongocrypt_status_message(status, len=nil)
      #   @api private
      #
      #   Returns the status message.
      #   @param [ FFI::Pointer ] status A pointer to a mongocrypt_status_t.
      #   @param [ FFI::Pointer | nil ] len (out param) An optional pointer to a
      #     uint32, where the length of the retun string will be written.
      #   @return [ String ] The status message.
      attach_function :mongocrypt_status_message, [:pointer, :pointer], :string

      # @!method self.mongocrypt_status_ok(status)
      #   @api private
      #
      #   Returns whether the status is ok or an error.
      #   @param [ FFI::Pointer ] status A pointer to a mongocrypt_status_t.
      #   @return [ Boolean ] Whether the status is ok.
      attach_function :mongocrypt_status_ok, [:pointer], :bool

      # @!method self.mongocrypt_status_destroy(status)
      #   @api private
      #
      #   Destroys the reference to the mongocrypt_status_t object.
      #   @param [ FFI::Pointer ] status A pointer to a mongocrypt_status_t.
      #   @return [ nil ] Always nil.
      attach_function :mongocrypt_status_destroy, [:pointer], :void

      # Enum labeling the various log levels
      enum :log_level, [
        :fatal,   0,
        :error,   1,
        :warn,    2,
        :info,    3,
        :debug,   4,
      ]

      # @!method mongocrypt_log_fn_t(level, message, len, ctx)
      #   @api private
      #
      #   A callback to the mongocrypt log function. Set a custom log callback
      #     with the mongocrypt_setopt_log_handler method
      #   @param [ Symbol ] level The log level; possible values defined by the
      #     log_level enum
      #   @param [ String ] message The log message
      #   @param [ Integer ] len The length of the message param, or -1 if the
      #     string is null terminated
      #   @param [ FFI::Pointer | nil ] ctx An optional pointer to a context
      #     object when this callback was set
      #   @return [ nil ] Always nil.
      #
      #   @note This defines a method signature for an FFI callback; it is not
      #     an instance method on the Binding class.
      callback :mongocrypt_log_fn_t, [:log_level, :string, :int, :pointer], :void

      # @!method self.ongocrypt_new
      #   @api private
      #
      #   Creates a new mongocrypt_t object.
      #   @return [ FFI::Pointer ] A pointer to a new mongocrypt_t object.
      attach_function :mongocrypt_new, [], :pointer

      # @!method self.mongocrypt_setopt_log_handler(crypt, log_fn, log_ctx=nil)
      #   @api private
      #
      #   Set the handler on the mongocrypt_t object to be called every time
      #     libmongocrypt logs a message.
      #   @param [ FFI::Pointer ] crypt A pointer to a mongocrypt_t object.
      #   @param [ Method ] log_fn A logging callback method.
      #   @param [ FFI::Pointer | nil ] log_ctx An optional pointer to a context
      #     to be passed into the log callback on every invocation.
      #   @return [ Boolean ] Whether setting the callback was successful.
      attach_function(
        :mongocrypt_setopt_log_handler,
        [:pointer, :mongocrypt_log_fn_t, :pointer],
        :bool
      )

      # Set the logger callback function on the Mongo::Crypt::Handle object
      #
      # @param [ Mongo::Crypt::Handle ] handle
      # @param [ Method ] log_callback
      #
      # @raise [ Mongo::Error::CryptError ] If the callback is not set successfully
      def self.setopt_log_handler(handle, log_callback)
        check_status(handle) do
          mongocrypt_setopt_log_handler(handle, log_callback, nil)
        end
      end

      # @!method self.mongocrypt_setopt_kms_providers(crypt, kms_providers)
      #   @api private
      #
      #   Configure KMS providers with a BSON document.
      #   @param [ FFI::Pointer ] crypt A pointer to a mongocrypt_t object.
      #   @param [ FFI::Pointer ] kms_providers A pointer to a
      #     mongocrypt_binary_t object that references a BSON document mapping
      #     the KMS provider names to credentials.
      #   @note Do not initialize ctx before calling this method.
      #
      #   @returns [ true | false ] Returns whether the options was set successfully.
      attach_function(
        :mongocrypt_setopt_kms_providers,
        [:pointer, :pointer],
        :bool
      )

      # Set KMS providers options on the Mongo::Crypt::Handle object
      #
      # @param [ Mongo::Crypt::Handle ] handle
      # @param [ BSON::Document ] kms_providers BSON document mapping
      #   the KMS provider names to credentials.
      #
      # @raise [ Mongo::Error::CryptError ] If the option is not set successfully
      def self.setopt_kms_providers(handle, kms_providers)
        validate_document(kms_providers)
        data = kms_providers.to_bson.to_s
        Binary.wrap_string(data) do |data_p|
          check_status(handle) do
            mongocrypt_setopt_kms_providers(handle.ref, data_p)
          end
        end
      end

      # @!method self.mongocrypt_setopt_schema_map(crypt, schema_map)
      #   @api private
      #
      #   Sets a local schema map for encryption.
      #   @param [ FFI::Pointer ] crypt A pointer to a mongocrypt_t object.
      #   @param [ FFI::Pointer ] schema_map A pointer to a mongocrypt_binary_t.
      #     object that references the schema map as a BSON binary string.
      #   @return [ Boolean ] Returns whether the option was set successfully.
      attach_function :mongocrypt_setopt_schema_map, [:pointer, :pointer], :bool

      # Set schema map on the Mongo::Crypt::Handle object
      #
      # @param [ Mongo::Crypt::Handle ] handle
      # @param [ BSON::Document ] schema_map_doc The schema map as a
      #   BSON::Document object
      #
      # @raise [ Mongo::Error::CryptError ] If the schema map is not set successfully
      def self.setopt_schema_map(handle, schema_map_doc)
        validate_document(schema_map_doc)
        data = schema_map_doc.to_bson.to_s
        Binary.wrap_string(data) do |data_p|
          check_status(handle) do
            mongocrypt_setopt_schema_map(handle.ref, data_p)
          end
        end
      end

      # @!method self.mongocrypt_init(crypt)
      #   @api private
      #
      #   Initialize the mongocrypt_t object.
      #   @param [ FFI::Pointer ] crypt A pointer to a mongocrypt_t object.
      #   @return [ Boolean ] Returns whether the crypt was initialized successfully.
      attach_function :mongocrypt_init, [:pointer], :bool

      # Initialize the Mongo::Crypt::Handle object
      #
      # @param [ Mongo::Crypt::Handle ] handle
      #
      # @raise [ Mongo::Error::CryptError ] If initialization fails
      def self.init(handle)
        check_status(handle) do
          mongocrypt_init(handle.ref)
        end
      end

      # @!method self.mongocrypt_status(crypt, status)
      #   @api private
      #
      #   Set the status information from the mongocrypt_t object on the
      #     mongocrypt_status_t object.
      #   @param [ FFI::Pointer ] crypt A pointer to a mongocrypt_t object.
      #   @param [ FFI::Pointer ] status A pointer to a mongocrypt_status_t object.
      #   @return [ Boolean ] Whether the status was successfully set.
      attach_function :mongocrypt_status, [:pointer, :pointer], :bool

      # @!method self.mongocrypt_destroy(crypt)
      #   @api private
      #
      #   Destroy the reference the mongocrypt_t object.
      #   @param [ FFI::Pointer ] crypt A pointer to a mongocrypt_t object.
      #   @return [ nil ] Always nil.
      attach_function :mongocrypt_destroy, [:pointer], :void

      # @!method self.mongocrypt_ctx_new(crypt)
      #   @api private
      #
      #   Create a new mongocrypt_ctx_t object (a wrapper for the libmongocrypt
      #     state machine).
      #   @param [ FFI::Pointer ] crypt A pointer to a mongocrypt_t object.
      #   @return [ FFI::Pointer ] A new mongocrypt_ctx_t object.
      attach_function :mongocrypt_ctx_new, [:pointer], :pointer

      # @!method self.mongocrypt_ctx_status(ctx, status)
      #   @api private
      #
      #   Set the status information from the mongocrypt_ctx_t object on the
      #     mongocrypt_status_t object.
      #   @param [ FFI::Pointer ] ctx A pointer to a mongocrypt_ctx_t object.
      #   @param [ FFI::Pointer ] status A pointer to a mongocrypt_status_t object.
      #   @return [ Boolean ] Whether the status was successfully set.
      attach_function :mongocrypt_ctx_status, [:pointer, :pointer], :bool

      # @!method self.mongocrypt_ctx_setopt_key_id(ctx, key_id)
      #   @api private
      #
      #   Set the key id used for explicit encryption.
      #   @param [ FFI::Pointer ] ctx A pointer to a mongocrypt_ctx_t object.
      #   @param [ FFI::Pointer ] key_id A pointer to a mongocrypt_binary_t object
      #     that references the 16-byte key-id.
      #   @note Do not initialize ctx before calling this method.
      #   @return [ Boolean ] Whether the option was successfully set.
      attach_function :mongocrypt_ctx_setopt_key_id, [:pointer, :pointer], :bool

      # Sets the key id option on an explicit encryption context.
      #
      # @param [ Mongo::Crypt::Context ] context Explicit encryption context
      # @param [ String ] key_id The key id
      #
      # @raise [ Mongo::Error::CryptError ] If the operation failed
      def self.ctx_setopt_key_id(context, key_id)
        Binary.wrap_string(key_id) do |key_id_p|
          check_ctx_status(context) do
            mongocrypt_ctx_setopt_key_id(context.ctx_p, key_id_p)
          end
        end
      end

      # @!method self.mongocrypt_ctx_setopt_key_alt_name(ctx, binary)
      #   @api private
      #
      #   When creating a data key, set an alternate name on that key. When
      #     performing explicit encryption, specifying which data key to use for
      #     encryption based on its keyAltName field.
      #   @param [ FFI::Pointer ] ctx A pointer to a mongocrypt_ctx_t object.
      #   @param [ FFI::Pointer ] binary A pointer to a mongocrypt_binary_t
      #     object that references a BSON document in the format
      #     { "keyAltName": <BSON UTF8 value> }.
      #   @return [ Boolean ] Whether the alternative name was successfully set.
      #   @note Do not initialize ctx before calling this method.
      attach_function(
        :mongocrypt_ctx_setopt_key_alt_name,
        [:pointer, :pointer],
        :bool
      )

      # Set multiple alternate key names on data key creation
      #
      # @param [ Mongo::Crypt::Context ] context A DataKeyContext
      # @param [ Array ] key_alt_names An array of alternate key names as strings
      #
      # @raise [ Mongo::Error::CryptError ] If any of the alternate names are
      #   not valid UTF8 strings
      def self.ctx_setopt_key_alt_names(context, key_alt_names)
        key_alt_names.each do |key_alt_name|
          key_alt_name_bson = { :keyAltName => key_alt_name }.to_bson.to_s

          Binary.wrap_string(key_alt_name_bson) do |key_alt_name_p|
            check_ctx_status(context) do
              mongocrypt_ctx_setopt_key_alt_name(context.ctx_p, key_alt_name_p)
            end
          end
        end
      end

      # @!method self.mongocrypt_ctx_setopt_key_material(ctx, binary)
      #   @api private
      #
      #   When creating a data key, set a custom key material to use for
      #     encrypting data.
      #   @param [ FFI::Pointer ] ctx A pointer to a mongocrypt_ctx_t object.
      #   @param [ FFI::Pointer ] binary A pointer to a mongocrypt_binary_t
      #     object that references the data encryption key to use.
      #   @return [ Boolean ] Whether the custom key material was successfully set.
      #   @note Do not initialize ctx before calling this method.
      attach_function(
        :mongocrypt_ctx_setopt_key_material,
        [:pointer, :pointer],
        :bool
      )

      # Set set a custom key material to use for
      #     encrypting data.
      #
      # @param [ Mongo::Crypt::Context ] context A DataKeyContext
      # @param [ BSON::Binary ] key_material 96 bytes of custom key material
      #
      # @raise [ Mongo::Error::CryptError ] If the key material is not 96 bytes.
      def self.ctx_setopt_key_material(context, key_material)
        data = {'keyMaterial' => key_material}.to_bson.to_s
        Binary.wrap_string(data) do |data_p|
          check_ctx_status(context) do
            mongocrypt_ctx_setopt_key_material(context.ctx_p, data_p)
          end
        end
      end

      # @!method self.mongocrypt_ctx_setopt_algorithm(ctx, algorithm, len)
      #   @api private
      #
      #   Set the algorithm used for explicit encryption.
      #   @param [ FFI::Pointer ] ctx A pointer to a mongocrypt_ctx_t object.
      #   @param [ String ] algorithm The algorithm name. Valid values are:
      #     - "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic"
      #     - "AEAD_AES_256_CBC_HMAC_SHA_512-Random"
      #   @param [ Integer ] len The length of the algorithm string.
      #   @note Do not initialize ctx before calling this method.
      #   @return [ Boolean ] Whether the option was successfully set.
      attach_function(
        :mongocrypt_ctx_setopt_algorithm,
        [:pointer, :string, :int],
        :bool
      )

      # Set the algorithm on the context
      #
      # @param [ Mongo::Crypt::Context ] context
      # @param [ String ] name The algorithm name. Valid values are:
      #   - "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic"
      #   - "AEAD_AES_256_CBC_HMAC_SHA_512-Random"
      #
      # @raise [ Mongo::Error::CryptError ] If the operation failed
      def self.ctx_setopt_algorithm(context, name)
        check_ctx_status(context) do
          mongocrypt_ctx_setopt_algorithm(context.ctx_p, name, -1)
        end
      end

      # @!method self.mongocrypt_ctx_setopt_key_encryption_key(ctx)
      #   @api private
      #
      #   Set key encryption key document for creating a data key.
      #   @param [ FFI::Pointer ] ctx A pointer to a mongocrypt_ctx_t object.
      #   @param [ FFI::Pointer ] bin A pointer to a mongocrypt_binary_t
      #     object that references a BSON document representing the key
      #     encryption key document with an additional "provider" field.
      #   @note Do not initialize ctx before calling this method.
      #   @return [ Boolean ] Whether the option was successfully set.
      attach_function(
        :mongocrypt_ctx_setopt_key_encryption_key,
        [:pointer, :pointer],
        :bool
      )

      # Set key encryption key document for creating a data key.
      #
      # @param [ Mongo::Crypt::Context ] context
      # @param [ BSON::Document ] key_document BSON document representing the key
      #     encryption key document with an additional "provider" field.
      #
      # @raise [ Mongo::Error::CryptError ] If the operation failed
      def self.ctx_setopt_key_encryption_key(context, key_document)
        validate_document(key_document)
        data = key_document.to_bson.to_s
        Binary.wrap_string(data) do |data_p|
          check_ctx_status(context) do
            mongocrypt_ctx_setopt_key_encryption_key(context.ctx_p, data_p)
          end
        end
      end

      # @!method self.mongocrypt_ctx_datakey_init(ctx)
      #   @api private
      #
      #   Initializes the ctx to create a data key.
      #   @param [ FFI::Pointer ] ctx A pointer to a mongocrypt_ctx_t object.
      #   @note Before calling this method, master key options must be set.
      #     Set AWS master key by calling mongocrypt_ctx_setopt_masterkey_aws
      #     and mongocrypt_ctx_setopt_masterkey_aws_endpoint. Set local master
      #     key by calling mongocrypt_ctx_setopt_masterkey_local.
      #   @return [ Boolean ] Whether the initialization was successful.
      attach_function :mongocrypt_ctx_datakey_init, [:pointer], :bool

      # Initialize the Context to create a data key
      #
      # @param [ Mongo::Crypt::Context ] context
      #
      # @raise [ Mongo::Error::CryptError ] If initialization fails
      def self.ctx_datakey_init(context)
        check_ctx_status(context) do
          mongocrypt_ctx_datakey_init(context.ctx_p)
        end
      end

      # @!method self.mongocrypt_ctx_datakey_init(ctx, filter)
      #   @api private
      #
      # Initialize a context to rewrap datakeys.
      #
      # @param [ FFI::Pointer ] ctx A pointer to a mongocrypt_ctx_t object.
      # @param [ FFI::Pointer ] filter A pointer to a  mongocrypt_binary_t object
      #   that represents filter to use for the find command on the key vault
      #   collection to retrieve datakeys to rewrap.
      #
      # @return [ Boolean ] Whether the initialization was successful.
      attach_function(
        :mongocrypt_ctx_rewrap_many_datakey_init,
        [:pointer, :pointer],
        :bool
      )

      # Initialize a context to rewrap datakeys.
      #
      # @param [ Mongo::Crypt::Context ] context
      # @param [ BSON::Document ] filter BSON Document
      #   that represents filter to use for the find command on the key vault
      #   collection to retrieve datakeys to rewrap.
      #
      # @return [ Boolean ] Whether the initialization was successful.
      def self.ctx_rewrap_many_datakey_init(context, filter)
        filter_data = filter.to_bson.to_s
        Binary.wrap_string(filter_data) do |data_p|
          check_ctx_status(context) do
            mongocrypt_ctx_rewrap_many_datakey_init(context.ctx_p, data_p)
          end
        end
      end

      # @!method self.mongocrypt_ctx_encrypt_init(ctx, db, db_len, cmd)
      #   @api private
      #
      #   Initializes the ctx for auto-encryption.
      #   @param [ FFI::Pointer ] ctx A pointer to a mongocrypt_ctx_t object.
      #   @param [ String ] db The database name.
      #   @param [ Integer ] db_len The length of the database name argument
      #     (or -1 for a null-terminated string).
      #   @param [ FFI::Pointer ] cmd A pointer to a mongocrypt_binary_t object
      #     that references the database command as a binary string.
      #   @note This method expects the passed-in BSON to be in the format:
      #     { "v": BSON value to decrypt }.
      #   @return [ Boolean ] Whether the initialization was successful.
      attach_function(
        :mongocrypt_ctx_encrypt_init,
        [:pointer, :string, :int, :pointer],
        :bool
      )

      # Initialize the Context for auto-encryption
      #
      # @param [ Mongo::Crypt::Context ] context
      # @param [ String ] db_name The name of the database against which the
      #   encrypted command is being performed
      # @param [ Hash ] command The command to be encrypted
      #
      # @raise [ Mongo::Error::CryptError ] If initialization fails
      def self.ctx_encrypt_init(context, db_name, command)
        validate_document(command)
        data = command.to_bson.to_s
        Binary.wrap_string(data) do |data_p|
          check_ctx_status(context) do
            mongocrypt_ctx_encrypt_init(context.ctx_p, db_name, -1, data_p)
          end
        end
      end

      # @!method self.mongocrypt_ctx_explicit_encrypt_init(ctx, msg)
      #   @api private
      #
      #   Initializes the ctx for explicit encryption.
      #   @param [ FFI::Pointer ] ctx A pointer to a mongocrypt_ctx_t object.
      #   @param [ FFI::Pointer ] msg A pointer to a mongocrypt_binary_t object
      #     that references the message to be encrypted as a binary string.
      #   @note Before calling this method, set a key_id, key_alt_name (optional),
      #     and encryption algorithm using the following methods:
      #     mongocrypt_ctx_setopt_key_id, mongocrypt_ctx_setopt_key_alt_name,
      #     and mongocrypt_ctx_setopt_algorithm.
      #   @return [ Boolean ] Whether the initialization was successful.
      attach_function(
        :mongocrypt_ctx_explicit_encrypt_init,
        [:pointer, :pointer],
        :bool
      )

      # Initialize the Context for explicit encryption
      #
      # @param [ Mongo::Crypt::Context ] context
      # @param [ Hash ] doc A BSON document to encrypt
      #
      # @raise [ Mongo::Error::CryptError ] If initialization fails
      def self.ctx_explicit_encrypt_init(context, doc)
        validate_document(doc)
        data = doc.to_bson.to_s
        Binary.wrap_string(data) do |data_p|
          check_ctx_status(context) do
            mongocrypt_ctx_explicit_encrypt_init(context.ctx_p, data_p)
          end
        end
      end

      # @!method self.mongocrypt_ctx_explicit_encrypt_init(ctx, msg)
      #   @api private
      #
      #   Initializes the ctx for explicit expression encryption.
      #   @param [ FFI::Pointer ] ctx A pointer to a mongocrypt_ctx_t object.
      #   @param [ FFI::Pointer ] msg A pointer to a mongocrypt_binary_t object
      #     that references the message to be encrypted as a binary string.
      #   @note Before calling this method, set a key_id, key_alt_name (optional),
      #     and encryption algorithm using the following methods:
      #     mongocrypt_ctx_setopt_key_id, mongocrypt_ctx_setopt_key_alt_name,
      #     and mongocrypt_ctx_setopt_algorithm.
      #   @return [ Boolean ] Whether the initialization was successful.
      attach_function(
        :mongocrypt_ctx_explicit_encrypt_expression_init,
        [:pointer, :pointer],
        :bool
      )

      # Initialize the Context for explicit expression encryption.
      #
      # @param [ Mongo::Crypt::Context ] context
      # @param [ Hash ] doc A BSON document to encrypt
      #
      # @raise [ Mongo::Error::CryptError ] If initialization fails
      def self.ctx_explicit_encrypt_expression_init(context, doc)
        validate_document(doc)
        data = doc.to_bson.to_s
        Binary.wrap_string(data) do |data_p|
          check_ctx_status(context) do
            mongocrypt_ctx_explicit_encrypt_expression_init(context.ctx_p, data_p)
          end
        end
      end

      # @!method self.mongocrypt_ctx_decrypt_init(ctx, doc)
      #   @api private
      #
      #   Initializes the ctx for auto-decryption.
      #   @param [ FFI::Pointer ] ctx A pointer to a mongocrypt_ctx_t object.
      #   @param [ FFI::Pointer ] doc A pointer to a mongocrypt_binary_t object
      #     that references the document to be decrypted as a BSON binary string.
      #   @return [ Boolean ] Whether the initialization was successful.
      attach_function :mongocrypt_ctx_decrypt_init, [:pointer, :pointer], :bool

      # Initialize the Context for auto-decryption
      #
      # @param [ Mongo::Crypt::Context ] context
      # @param [ BSON::Document ] command A BSON document to decrypt
      #
      # @raise [ Mongo::Error::CryptError ] If initialization fails
      def self.ctx_decrypt_init(context, command)
        validate_document(command)
        data = command.to_bson.to_s
        Binary.wrap_string(data) do |data_p|
          check_ctx_status(context) do
            mongocrypt_ctx_decrypt_init(context.ctx_p, data_p)
          end
        end
      end

      # @!method self.mongocrypt_ctx_explicit_decrypt_init(ctx, msg)
      #   @api private
      #
      #   Initializes the ctx for explicit decryption.
      #   @param [ FFI::Pointer ] ctx A pointer to a mongocrypt_ctx_t object.
      #   @param [ FFI::Pointer ] msg A pointer to a mongocrypt_binary_t object
      #     that references the message to be decrypted as a BSON binary string.
      #   @return [ Boolean ] Whether the initialization was successful.
      attach_function(
        :mongocrypt_ctx_explicit_decrypt_init,
        [:pointer, :pointer],
        :bool
      )

      # Initialize the Context for explicit decryption
      #
      # @param [ Mongo::Crypt::Context ] context
      # @param [ Hash ] doc A BSON document to decrypt
      #
      # @raise [ Mongo::Error::CryptError ] If initialization fails
      def self.ctx_explicit_decrypt_init(context, doc)
        validate_document(doc)
        data = doc.to_bson.to_s
        Binary.wrap_string(data) do |data_p|
          check_ctx_status(context) do
            mongocrypt_ctx_explicit_decrypt_init(context.ctx_p, data_p)
          end
        end
      end

      # An enum labeling different libmognocrypt state machine states
      enum :mongocrypt_ctx_state, [
        :error,                 0,
        :need_mongo_collinfo,   1,
        :need_mongo_markings,   2,
        :need_mongo_keys,       3,
        :need_kms,              4,
        :ready,                 5,
        :done,                  6,
        :need_kms_credentials,  7,
      ]

      # @!method self.mongocrypt_ctx_state(ctx)
      #   @api private
      #
      #   Get the current state of the ctx.
      #   @param [ FFI::Pointer ] ctx A pointer to a mongocrypt_ctx_t object.
      #   @return [ Symbol ] The current state, will be one of the values defined
      #     by the mongocrypt_ctx_state enum.
      attach_function :mongocrypt_ctx_state, [:pointer], :mongocrypt_ctx_state

      # @!method self.mongocrypt_ctx_mongo_op(ctx, op_bson)
      #   @api private
      #
      #   Get a BSON operation for the driver to run against the MongoDB
      #     collection, the key vault database, or mongocryptd.
      #   @param [ FFI::Pointer ] ctx A pointer to a mongocrypt_ctx_t object.
      #   @param [ FFI::Pointer ] op_bson (out param) A pointer to a
      #     mongocrypt_binary_t object that will have a reference to the
      #     BSON operation written to it by libmongocrypt.
      #   @return [ Boolean ] A boolean indicating the success of the operation.
      attach_function :mongocrypt_ctx_mongo_op, [:pointer, :pointer], :bool

      # Returns a BSON::Document representing an operation that the
      # driver must perform on behalf of libmongocrypt to get the
      # information it needs in order to continue with
      # encryption/decryption (for example, a filter for a key vault query).
      #
      # @param [ Mongo::Crypt::Context ] context
      #
      # @raise [ Mongo::Crypt ] If there is an error getting the operation
      # @return [ BSON::Document ] The operation that the driver must perform
      def self.ctx_mongo_op(context)
        binary = Binary.new

        check_ctx_status(context) do
          mongocrypt_ctx_mongo_op(context.ctx_p, binary.ref)
        end

        # TODO since the binary references a C pointer, and ByteBuffer is
        # written in C in MRI, we could omit a copy of the data by making
        # ByteBuffer reference the string that is owned by libmongocrypt.
        BSON::Document.from_bson(BSON::ByteBuffer.new(binary.to_s), mode: :bson)
      end

      # @!method self.mongocrypt_ctx_mongo_feed(ctx, reply)
      #   @api private
      #
      #   Feed a BSON reply to libmongocrypt.
      #   @param [ FFI::Pointer ] ctx A pointer to a mongocrypt_ctx_t object.
      #   @param [ FFI::Pointer ] reply A mongocrypt_binary_t object that
      #     references the BSON reply to feed to libmongocrypt.
      #   @return [ Boolean ] A boolean indicating the success of the operation.
      attach_function :mongocrypt_ctx_mongo_feed, [:pointer, :pointer], :bool

      # Feed a response from the driver back to libmongocrypt
      #
      # @param [ Mongo::Crypt::Context ] context
      # @param [ BSON::Document ] doc The document representing the response
      #
      # @raise [ Mongo::Error::CryptError ] If the response is not fed successfully
      def self.ctx_mongo_feed(context, doc)
        validate_document(doc)
        data = doc.to_bson.to_s
        Binary.wrap_string(data) do |data_p|
          check_ctx_status(context) do
            mongocrypt_ctx_mongo_feed(context.ctx_p, data_p)
          end
        end
      end

      # @!method self.mongocrypt_ctx_mongo_done(ctx)
      #   @api private
      #
      #   Indicate to libmongocrypt that the driver is done feeding replies.
      #   @param [ FFI::Pointer ] ctx A pointer to a mongocrypt_ctx_t object.
      #   @return [ Boolean ] A boolean indicating the success of the operation.
      attach_function :mongocrypt_ctx_mongo_done, [:pointer], :bool

      # @!method self.mongocrypt_ctx_mongo_next_kms_ctx(ctx)
      #   @api private
      #
      #   Return a pointer to a mongocrypt_kms_ctx_t object or NULL.
      #   @param [ FFI::Pointer ] ctx A pointer to a mongocrypt_ctx_t object.
      #   @return [ FFI::Pointer ] A pointer to a mongocrypt_kms_ctx_t object.
      attach_function :mongocrypt_ctx_next_kms_ctx, [:pointer], :pointer

      # Return a new KmsContext object needed by a Context object.
      #
      # @param [ Mongo::Crypt::Context ] context
      #
      # @return [ Mongo::Crypt::KmsContext | nil ] The KmsContext needed to
      #   fetch an AWS master key or nil, if no KmsContext is needed
      def self.ctx_next_kms_ctx(context)
        kms_ctx_p = mongocrypt_ctx_next_kms_ctx(context.ctx_p)

        if kms_ctx_p.null?
          nil
        else
          KmsContext.new(kms_ctx_p)
        end
      end

      # @!method self.mongocrypt_kms_ctx_get_kms_provider(crypt, kms_providers)
      #   @api private
      #
      # Get the KMS provider identifier associated with this KMS request.
      #
      # This is used to conditionally configure TLS connections based on the KMS
      # request. It is useful for KMIP, which authenticates with a client
      # certificate.
      #
      # @param [ FFI::Pointer ] kms Pointer mongocrypt_kms_ctx_t object.
      # @param [ FFI::Pointer ] len (outparam) Receives the length of the
      #   returned string. It may be NULL. If it is not NULL, it is set to
      #   the length of the returned string without the NULL terminator.
      #
      # @returns [ FFI::Pointer ] One of the NULL terminated static strings: "aws", "azure", "gcp", or
      # "kmip".
      attach_function(
        :mongocrypt_kms_ctx_get_kms_provider,
        [:pointer, :pointer],
        :pointer
      )

      # Get the KMS provider identifier associated with this KMS request.
      #
      # This is used to conditionally configure TLS connections based on the KMS
      # request. It is useful for KMIP, which authenticates with a client
      # certificate.
      #
      # @param [ FFI::Pointer ] kms Pointer mongocrypt_kms_ctx_t object.
      #
      # @returns [ Symbol | nil ] KMS provider identifier.
      def self.kms_ctx_get_kms_provider(kms_context)
        len_ptr = FFI::MemoryPointer.new(:uint32, 1)
        provider = mongocrypt_kms_ctx_get_kms_provider(
          kms_context.kms_ctx_p,
          len_ptr
        )
        if len_ptr.nil?
          nil
        else
          len = if BSON::Environment.jruby?
            # JRuby FFI implementation does not have `read(type)` method, but it
            # has this `get_uint32`.
            len_ptr.get_uint32
          else
            # For MRI we use a documented `read` method - https://www.rubydoc.info/github/ffi/ffi/FFI%2FPointer:read
            len_ptr.read(:uint32)
          end
          provider.read_string(len).to_sym
        end
      end

      # @!method self.mongocrypt_kms_ctx_message(kms, msg)
      #   @api private
      #
      #   Get the message needed to fetch the AWS KMS master key.
      #   @param [ FFI::Pointer ] kms Pointer to the mongocrypt_kms_ctx_t object
      #   @param [ FFI::Pointer ] msg (outparam) Pointer to a mongocrypt_binary_t
      #     object that will have the location of the message written to it by
      #     libmongocrypt.
      #   @return [ Boolean ] Whether the operation is successful.
      attach_function :mongocrypt_kms_ctx_message, [:pointer, :pointer], :bool

      # Get the HTTP message needed to fetch the AWS KMS master key from a
      # KmsContext object.
      #
      # @param [ Mongo::Crypt::KmsContext ] kms_context
      #
      # @raise [ Mongo::Error::CryptError ] If the response is not fed successfully
      #
      # @return [ String ] The HTTP message
      def self.kms_ctx_message(kms_context)
        binary = Binary.new

        check_kms_ctx_status(kms_context) do
          mongocrypt_kms_ctx_message(kms_context.kms_ctx_p, binary.ref)
        end

        return binary.to_s
      end

      # @!method self.mongocrypt_kms_ctx_endpoint(kms, endpoint)
      #   @api private
      #
      #   Get the hostname with which to connect over TLS to get information about
      #     the AWS master key.
      #   @param [ FFI::Pointer ] kms A pointer to a mongocrypt_kms_ctx_t object.
      #   @param [ FFI::Pointer ] endpoint (out param) A pointer to which the
      #     endpoint string will be written by libmongocrypt.
      #   @return [ Boolean ] Whether the operation was successful.
      attach_function :mongocrypt_kms_ctx_endpoint, [:pointer, :pointer], :bool

      # Get the hostname with which to connect over TLS to get information
      # about the AWS master key.
      #
      # @param [ Mongo::Crypt::KmsContext ] kms_context
      #
      # @raise [ Mongo::Error::CryptError ] If the response is not fed successfully
      #
      # @return [ String | nil ] The hostname, or nil if none exists
      def self.kms_ctx_endpoint(kms_context)
        ptr = FFI::MemoryPointer.new(:pointer, 1)

        check_kms_ctx_status(kms_context) do
          mongocrypt_kms_ctx_endpoint(kms_context.kms_ctx_p, ptr)
        end

        str_ptr = ptr.read_pointer
        str_ptr.null? ? nil : str_ptr.read_string.force_encoding('UTF-8')
      end

      # @!method self.mongocrypt_kms_ctx_bytes_needed(kms)
      #   @api private
      #
      #   Get the number of bytes needed by the KMS context.
      #   @param [ FFI::Pointer ] kms The mongocrypt_kms_ctx_t object.
      #   @return [ Integer ] The number of bytes needed.
      attach_function :mongocrypt_kms_ctx_bytes_needed, [:pointer], :int

      # Get the number of bytes needed by the KmsContext.
      #
      # @param [ Mongo::Crypt::KmsContext ] kms_context
      #
      # @return [ Integer ] The number of bytes needed
      def self.kms_ctx_bytes_needed(kms_context)
        mongocrypt_kms_ctx_bytes_needed(kms_context.kms_ctx_p)
      end

      # @!method self.mongocrypt_kms_ctx_feed(kms, bytes)
      #   @api private
      #
      #   Feed replies from the KMS back to libmongocrypt.
      #   @param [ FFI::Pointer ] kms A pointer to the mongocrypt_kms_ctx_t object.
      #   @param [ FFI::Pointer ] bytes A pointer to a mongocrypt_binary_t
      #     object that references the response from the KMS.
      #   @return [ Boolean ] Whether the operation was successful.
      attach_function :mongocrypt_kms_ctx_feed, [:pointer, :pointer], :bool

      # Feed replies from the KMS back to libmongocrypt.
      #
      # @param [ Mongo::Crypt::KmsContext ] kms_context
      # @param [ String ] bytes The data to feed to libmongocrypt
      #
      # @raise [ Mongo::Error::CryptError ] If the response is not fed successfully
      def self.kms_ctx_feed(kms_context, bytes)
        check_kms_ctx_status(kms_context) do
          Binary.wrap_string(bytes) do |bytes_p|
            mongocrypt_kms_ctx_feed(kms_context.kms_ctx_p, bytes_p)
          end
        end
      end

      # @!method self.mongocrypt_kms_ctx_status(kms, status)
      #   @api private
      #
      #   Write status information about the mongocrypt_kms_ctx_t object
      #     to the mongocrypt_status_t object.
      #   @param [ FFI::Pointer ] kms A pointer to the mongocrypt_kms_ctx_t object.
      #   @param [ FFI::Pointer ] status A pointer to a mongocrypt_status_t object.
      #   @return [ Boolean ] Whether the operation was successful.
      attach_function :mongocrypt_kms_ctx_status, [:pointer, :pointer], :bool

      # If the provided block returns false, raise a CryptError with the
      # status information from the provided KmsContext object.
      #
      # @param [ Mongo::Crypt::KmsContext ] kms_context
      #
      # @raise [ Mongo::Error::CryptError ] If the provided block returns false
      def self.check_kms_ctx_status(kms_context)
        unless yield
          status = Status.new

          mongocrypt_kms_ctx_status(kms_context.kms_ctx_p, status.ref)
          status.raise_crypt_error(kms: true)
        end
      end

      # @!method self.mongocrypt_kms_ctx_done(ctx)
      #   @api private
      #
      #   Indicate to libmongocrypt that it will receive no more replies from
      #     mongocrypt_kms_ctx_t objects.
      #   @param [ FFI::Pointer ] ctx A pointer to a mongocrypt_ctx_t object.
      #   @return [ Boolean ] Whether the operation was successful.
      attach_function :mongocrypt_ctx_kms_done, [:pointer], :bool

      # Indicate to libmongocrypt that it will receive no more KMS replies.
      #
      # @param [ Mongo::Crypt::Context ] context
      #
      # @raise [ Mongo::Error::CryptError ] If the operation is unsuccessful
      def self.ctx_kms_done(context)
        check_ctx_status(context) do
          mongocrypt_ctx_kms_done(context.ctx_p)
        end
      end

      # @!method self.mongocrypt_ctx_finalize(ctx, op_bson)
      #   @api private
      #
      #   Perform the final encryption or decryption and return a BSON document.
      #   @param [ FFI::Pointer ] ctx A pointer to a mongocrypt_ctx_t object.
      #   @param [ FFI::Pointer ] op_bson (out param) A pointer to a
      #     mongocrypt_binary_t object that will have a reference to the
      #     final encrypted BSON document.
      #   @return [ Boolean ] A boolean indicating the success of the operation.
      attach_function :mongocrypt_ctx_finalize, [:pointer, :pointer], :void

      # Finalize the state machine represented by the Context
      #
      # @param [ Mongo::Crypt::Context ] context
      #
      # @raise [ Mongo::Error::CryptError ] If the state machine is not successfully
      #   finalized
      def self.ctx_finalize(context)
        binary = Binary.new

        check_ctx_status(context) do
          mongocrypt_ctx_finalize(context.ctx_p, binary.ref)
        end

        # TODO since the binary references a C pointer, and ByteBuffer is
        # written in C in MRI, we could omit a copy of the data by making
        # ByteBuffer reference the string that is owned by libmongocrypt.
        BSON::Document.from_bson(BSON::ByteBuffer.new(binary.to_s), mode: :bson)
      end

      # @!method self.mongocrypt_ctx_destroy(ctx)
      #   @api private
      #
      #   Destroy the reference to the mongocrypt_ctx_t object.
      #   @param [ FFI::Pointer ] ctx A pointer to a mongocrypt_ctx_t object.
      #   @return [ nil ] Always nil.
      attach_function :mongocrypt_ctx_destroy, [:pointer], :void

      # @!method mongocrypt_crypto_fn(ctx, key, iv, input, output, status)
      #   @api private
      #
      #   A callback to a function that performs AES encryption or decryption.
      #   @param [ FFI::Pointer | nil] ctx An optional pointer to a context object
      #     that may have been set when hooks were enabled.
      #   @param [ FFI::Pointer ] key A pointer to a mongocrypt_binary_t object
      #     that references the 32-byte AES encryption key.
      #   @param [ FFI::Pointer ] iv A pointer to a mongocrypt_binary_t object
      #     that references the 16-byte AES IV.
      #   @param [ FFI::Pointer ] input A pointer to a mongocrypt_binary_t object
      #     that references the value to be encrypted/decrypted.
      #   @param [ FFI::Pointer ] output (out param) A pointer to a
      #     mongocrypt_binary_t object will have a reference to the encrypted/
      #     decrypted value written to it by libmongocrypt.
      #   @param [ FFI::Pointer ] status A pointer to a mongocrypt_status_t
      #     object to which an error message will be written if encryption fails.
      #   @return [ Bool ] Whether encryption/decryption was successful.
      #
      #   @note This defines a method signature for an FFI callback; it is not
      #     an instance method on the Binding class.
      callback(
        :mongocrypt_crypto_fn,
        [:pointer, :pointer, :pointer, :pointer, :pointer, :pointer, :pointer],
        :bool
      )

      # @!method mongocrypt_hmac_fn(ctx, key, input, output, status)
      #   @api private
      #
      #   A callback to a function that performs HMAC SHA-512 or SHA-256.
      #   @param [ FFI::Pointer | nil ] ctx An optional pointer to a context object
      #     that may have been set when hooks were enabled.
      #   @param [ FFI::Pointer ] key A pointer to a mongocrypt_binary_t object
      #     that references the 32-byte HMAC SHA encryption key.
      #   @param [ FFI::Pointer ] input A pointer to a mongocrypt_binary_t object
      #     that references the input value.
      #   @param [ FFI::Pointer ] output (out param) A pointer to a
      #     mongocrypt_binary_t object will have a reference to the output value
      #     written to it by libmongocrypt.
      #   @param [ FFI::Pointer ] status A pointer to a mongocrypt_status_t
      #     object to which an error message will be written if encryption fails.
      #   @return [ Bool ] Whether HMAC-SHA was successful.
      #
      #   @note This defines a method signature for an FFI callback; it is not
      #     an instance method on the Binding class.
      callback(
        :mongocrypt_hmac_fn,
        [:pointer, :pointer, :pointer, :pointer, :pointer],
        :bool
      )

      # @!method mongocrypt_hash_fn(ctx, input, output, status)
      #   @api private
      #
      #   A callback to a SHA-256 hash function.
      #   @param [ FFI::Pointer | nil ] ctx An optional pointer to a context object
      #     that may have been set when hooks were enabled.
      #   @param [ FFI::Pointer ] input A pointer to a mongocrypt_binary_t object
      #     that references the value to be hashed.
      #   @param [ FFI::Pointer ] output (out param) A pointer to a
      #     mongocrypt_binary_t object will have a reference to the output value
      #     written to it by libmongocrypt.
      #   @param [ FFI::Pointer ] status A pointer to a mongocrypt_status_t
      #     object to which an error message will be written if encryption fails.
      #   @return [ Bool ] Whether hashing was successful.
      #
      #   @note This defines a method signature for an FFI callback; it is not
      #     an instance method on the Binding class.
      callback :mongocrypt_hash_fn, [:pointer, :pointer, :pointer, :pointer], :bool

      # @!method mongocrypt_random_fn(ctx, output, count, status)
      #   @api private
      #
      #   A callback to a crypto secure random function.
      #   @param [ FFI::Pointer | nil ] ctx An optional pointer to a context object
      #     that may have been set when hooks were enabled.
      #   @param [ FFI::Pointer ] output (out param) A pointer to a
      #     mongocrypt_binary_t object will have a reference to the output value
      #     written to it by libmongocrypt.
      #   @param [ Integer ] count The number of random bytes to return.
      #   @param [ FFI::Pointer ] status A pointer to a mongocrypt_status_t
      #     object to which an error message will be written if encryption fails.
      #   @return [ Bool ] Whether hashing was successful.
      #
      #   @note This defines a method signature for an FFI callback; it is not
      #     an instance method on the Binding class.
      callback :mongocrypt_random_fn, [:pointer, :pointer, :int, :pointer], :bool

      # @!method self.mongocrypt_setopt_crypto_hooks(crypt, aes_enc_fn, aes_dec_fn, random_fn, sha_512_fn, sha_256_fn, hash_fn, ctx=nil)
      #   @api private
      #
      #   Set crypto hooks on the provided mongocrypt object.
      #   @param [ FFI::Pointer ] crypt A pointer to a mongocrypt_t object.
      #   @param [ Proc ] aes_enc_fn An AES encryption method.
      #   @param [ Proc ] aes_dec_fn An AES decryption method.
      #   @param [ Proc ] random_fn A random method.
      #   @param [ Proc ] sha_512_fn A HMAC SHA-512 method.
      #   @param [ Proc ] sha_256_fn A HMAC SHA-256 method.
      #   @param [ Proc ] hash_fn A SHA-256 hash method.
      #   @param [ FFI::Pointer | nil ] ctx An optional pointer to a context object
      #     that may have been set when hooks were enabled.
      #   @return [ Boolean ] Whether setting this option succeeded.
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

      # Set crypto callbacks on the Handle
      #
      # @param [ Mongo::Crypt::Handle ] handle
      # @param [ Method ] aes_encrypt_cb An AES encryption method
      # @param [ Method ] aes_decrypt_cb A AES decryption method
      # @param [ Method ] random_cb A method that returns a string of random bytes
      # @param [ Method ] hmac_sha_512_cb A HMAC SHA-512 method
      # @param [ Method ] hmac_sha_256_cb A HMAC SHA-256 method
      # @param [ Method ] hmac_hash_cb A SHA-256 hash method
      #
      # @raise [ Mongo::Error::CryptError ] If the callbacks aren't set successfully
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

      # @!method self.mongocrypt_setopt_crypto_hook_sign_rsaes_pkcs1_v1_5(crypt, sign_rsaes_pkcs1_v1_5, ctx=nil)
      #   @api private
      #
      #   Set a crypto hook for the RSASSA-PKCS1-v1_5 algorithm with a SHA-256 hash.
      #   @param [ FFI::Pointer ] crypt A pointer to a mongocrypt_t object.
      #   @param [ Proc ] sign_rsaes_pkcs1_v1_5 A RSASSA-PKCS1-v1_5 signing method.
      #   @param [ FFI::Pointer | nil ] ctx An optional pointer to a context object
      #     that may have been set when hooks were enabled.
      #   @return [ Boolean ] Whether setting this option succeeded.
      attach_function(
        :mongocrypt_setopt_crypto_hook_sign_rsaes_pkcs1_v1_5,
        [
          :pointer,
          :mongocrypt_hmac_fn,
          :pointer
        ],
        :bool
      )

      # Set a crypto hook for the RSASSA-PKCS1-v1_5 algorithm with
      #   a SHA-256 hash oh the Handle.
      #
      # @param [ Mongo::Crypt::Handle ] handle
      # @param [ Method ] rsaes_pkcs_signature_cb A RSASSA-PKCS1-v1_5 signing method.
      #
      # @raise [ Mongo::Error::CryptError ] If the callbacks aren't set successfully
      def self.setopt_crypto_hook_sign_rsaes_pkcs1_v1_5(
        handle,
        rsaes_pkcs_signature_cb
      )
        check_status(handle) do
          mongocrypt_setopt_crypto_hook_sign_rsaes_pkcs1_v1_5(
            handle.ref,
            rsaes_pkcs_signature_cb,
            nil
          )
        end
      end

      # @!method self.mongocrypt_setopt_encrypted_field_config_map(crypt, efc_map)
      #   @api private
      #
      # Set a local EncryptedFieldConfigMap for encryption.
      #
      # @param [ FFI::Pointer ] crypt A pointer to a mongocrypt_t object.
      # @param [ FFI::Pointer ] efc_map A pointer to mongocrypt_binary_t object that
      # references a BSON document representing the EncryptedFieldConfigMap
      # supplied by the user. The keys are collection namespaces and values are
      # EncryptedFieldConfigMap documents.
      #
      # @return [ Boolean ] Whether the operation succeeded.
      attach_function(
        :mongocrypt_setopt_encrypted_field_config_map,
        [
          :pointer,
          :pointer
        ],
        :bool
      )

      # Set a local EncryptedFieldConfigMap for encryption.
      #
      # @param [ Mongo::Crypt::Handle ] handle
      # @param [ BSON::Document ] efc_map A BSON document representing
      #   the EncryptedFieldConfigMap supplied by the user.
      #   The keys are collection namespaces and values are
      #   EncryptedFieldConfigMap documents.
      #
      # @raise [ Mongo::Error::CryptError ] If the operation failed.
      def self.setopt_encrypted_field_config_map(handle, efc_map)
        validate_document(efc_map)
        data = efc_map.to_bson.to_s
        Binary.wrap_string(data) do |data_p|
          check_status(handle) do
            mongocrypt_setopt_encrypted_field_config_map(
              handle.ref,
              data_p
            )
          end
        end
      end

      # @!method self.mongocrypt_setopt_bypass_query_analysis(crypt)
      #   @api private
      #
      # Opt into skipping query analysis.
      #
      # If opted in:
      # - The csfle shared library will not attempt to be loaded.
      # - A mongocrypt_ctx_t will never enter the MONGOCRYPT_CTX_NEED_MARKINGS state.
      #
      # @param [ FFI::Pointer ] crypt A pointer to a mongocrypt_t object.
      attach_function(:mongocrypt_setopt_bypass_query_analysis, [:pointer], :void)

      # Opt-into skipping query analysis.
      #
      # If opted in:
      # - The csfle shared library will not attempt to be loaded.
      # - A mongocrypt_ctx_t will never enter the MONGOCRYPT_CTX_NEED_MARKINGS state.
      #
      # @param [ Mongo::Crypt::Handle ] handle
      def self.setopt_bypass_query_analysis(handle)
        mongocrypt_setopt_bypass_query_analysis(handle.ref)
      end

      # @!method self.mongocrypt_setopt_aes_256_ctr(crypt, aes_256_ctr_encrypt, aes_256_ctr_decrypt, ctx)
      #   @api private
      #
      #   Set a crypto hook for the AES256-CTR operations.
      #
      #   @param [ FFI::Pointer ] crypt A pointer to a mongocrypt_t object.
      #   @param [ Proc ] aes_enc_fn An AES-CTR encryption method.
      #   @param [ Proc ] aes_dec_fn An AES-CTR decryption method.
      #   @param [ FFI::Pointer | nil ] ctx An optional pointer to a context object
      #     that may have been set when hooks were enabled.
      #   @return [ Boolean ] Whether setting this option succeeded.
      attach_function(
        :mongocrypt_setopt_aes_256_ctr,
        [
          :pointer,
          :mongocrypt_crypto_fn,
          :mongocrypt_crypto_fn,
          :pointer
        ],
        :bool
      )

      # Set a crypto hook for the AES256-CTR operations.
      #
      # @param [ Mongo::Crypt::Handle ] handle
      # @param [ Method ] aes_encrypt_cb An AES-CTR encryption method
      # @param [ Method ] aes_decrypt_cb A AES-CTR decryption method
      #
      # @raise [ Mongo::Error::CryptError ] If the callbacks aren't set successfully
      def self.setopt_aes_256_ctr(handle, aes_ctr_encrypt_cb, aes_ctr_decrypt_cb)
        check_status(handle) do
          mongocrypt_setopt_aes_256_ctr(handle.ref,
            aes_ctr_encrypt_cb, aes_ctr_decrypt_cb, nil
          )
        end
      end

      # @!method self.mongocrypt_setopt_append_crypt_shared_lib_search_path(crypt, path)
      #   @api private
      #
      # Append an additional search directory to the search path for loading
      #   the crypt_shared dynamic library.
      #
      # @param [ FFI::Pointer ] crypt A pointer to a mongocrypt_t object.
      # @param [ String ] path A path to search for the crypt shared library. If the leading element of
      #   the path is the literal string "$ORIGIN", that substring will be replaced
      #   with the directory path containing the executable libmongocrypt module. If
      #   the path string is literal "$SYSTEM", then libmongocrypt will defer to the
      #   system's library resolution mechanism to find the crypt_shared library.
      attach_function(
        :mongocrypt_setopt_append_crypt_shared_lib_search_path,
        [
          :pointer,
          :string,
        ],
        :void
      )

      # Append an additional search directory to the search path for loading
      #   the crypt_shared dynamic library.
      #
      # @param [ Mongo::Crypt::Handle ] handle
      # @param [ String ] path A search path for the crypt shared library.
      def self.setopt_append_crypt_shared_lib_search_path(handle, path)
        check_status(handle) do
          mongocrypt_setopt_append_crypt_shared_lib_search_path(handle.ref, path)
        end
      end

      # @!method self.mongocrypt_setopt_set_crypt_shared_lib_path_override(crypt, path)
      #   @api private
      #
      # Set a single override path for loading the crypt shared library.
      #
      # @param [ FFI::Pointer ] crypt A pointer to a mongocrypt_t object.
      # @param [ String ] path A path to crypt shared library file. If the leading element of
      #   the path is the literal string "$ORIGIN", that substring will be replaced
      #   with the directory path containing the executable libmongocrypt module.
      attach_function(
        :mongocrypt_setopt_set_crypt_shared_lib_path_override,
        [
          :pointer,
          :string,
        ],
        :void
      )

      # Set a single override path for loading the crypt shared library.
      #
      # @param [ Mongo::Crypt::Handle ] handle
      # @param [ String ] path A path to crypt shared library file.
      def self.setopt_set_crypt_shared_lib_path_override(handle, path)
        check_status(handle) do
          mongocrypt_setopt_set_crypt_shared_lib_path_override(handle.ref, path)
        end
      end

      # @!method self.mongocrypt_crypt_shared_lib_version(crypt)
      #   @api private
      #
      # Obtain a 64-bit constant encoding the version of the loaded
      # crypt_shared library, if available.
      #
      # The version is encoded as four 16-bit numbers, from high to low:
      #
      # - Major version
      # - Minor version
      # - Revision
      # - Reserved
      #
      # For example, version 6.2.1 would be encoded as: 0x0006'0002'0001'0000
      #
      # @param [ FFI::Pointer ] crypt A pointer to a mongocrypt_t object.
      #
      # @return [int64] A 64-bit encoded version number, with the version encoded as four
      #   sixteen-bit integers, or zero if no crypt_shared library was loaded.
      attach_function(
        :mongocrypt_crypt_shared_lib_version,
        [ :pointer ],
        :uint64
      )

      # Obtain a 64-bit constant encoding the version of the loaded
      # crypt_shared library, if available.
      #
      # The version is encoded as four 16-bit numbers, from high to low:
      #
      # - Major version
      # - Minor version
      # - Revision
      # - Reserved
      #
      # For example, version 6.2.1 would be encoded as: 0x0006'0002'0001'0000
      #
      # @param [ Mongo::Crypt::Handle ] handle
      #
      # @return [ Integer ] A 64-bit encoded version number, with the version encoded as four
      #   sixteen-bit integers, or zero if no crypt_shared library was loaded.
      def self.crypt_shared_lib_version(handle)
        mongocrypt_crypt_shared_lib_version(handle.ref)
      end

      # @!method self.mongocrypt_setopt_use_need_kms_credentials_state(crypt)
      #   @api private
      #
      # Opt-into handling the MONGOCRYPT_CTX_NEED_KMS_CREDENTIALS state.
      #
      # If set, before entering the MONGOCRYPT_CTX_NEED_KMS state,
      # contexts may enter the MONGOCRYPT_CTX_NEED_KMS_CREDENTIALS state
      # and then wait for credentials to be supplied through
      # `mongocrypt_ctx_provide_kms_providers`.
      #
      # A context will only enter MONGOCRYPT_CTX_NEED_KMS_CREDENTIALS
      # if an empty document was set for a KMS provider in
      # `mongocrypt_setopt_kms_providers`.
      #
      # @param [ FFI::Pointer ] crypt A pointer to a mongocrypt_t object.
      attach_function(
        :mongocrypt_setopt_use_need_kms_credentials_state,
        [ :pointer ],
        :void
      )

      # Opt-into handling the MONGOCRYPT_CTX_NEED_KMS_CREDENTIALS state.
      #
      # If set, before entering the MONGOCRYPT_CTX_NEED_KMS state,
      # contexts may enter the MONGOCRYPT_CTX_NEED_KMS_CREDENTIALS state
      # and then wait for credentials to be supplied through
      # `mongocrypt_ctx_provide_kms_providers`.
      #
      # A context will only enter MONGOCRYPT_CTX_NEED_KMS_CREDENTIALS
      # if an empty document was set for a KMS provider in
      # `mongocrypt_setopt_kms_providers`.
      #
      # @param [ Mongo::Crypt::Handle ] handle
      def self.setopt_use_need_kms_credentials_state(handle)
        mongocrypt_setopt_use_need_kms_credentials_state(handle.ref)
      end

      # @!method self.mongocrypt_ctx_provide_kms_providers(ctx, kms_providers)
      #   @api private
      #
      # Call in response to the MONGOCRYPT_CTX_NEED_KMS_CREDENTIALS state
      # to set per-context KMS provider settings. These follow the same format
      # as `mongocrypt_setopt_kms_providers``. If no keys are present in the
      # BSON input, the KMS provider settings configured for the mongocrypt_t
      # at initialization are used.
      #
      # @param [ FFI::Pointer ] ctx A pointer to a mongocrypt_ctx_t object.
      # @param [ FFI::Pointer ] kms_providers A pointer to a
      #   mongocrypt_binary_t object that references a BSON document mapping
      #   the KMS provider names to credentials.
      #
      # @returns [ true | false ] Returns whether the options was set successfully.
      attach_function(
        :mongocrypt_ctx_provide_kms_providers,
        [ :pointer, :pointer ],
        :bool
      )

      # Call in response to the MONGOCRYPT_CTX_NEED_KMS_CREDENTIALS state
      # to set per-context KMS provider settings. These follow the same format
      # as `mongocrypt_setopt_kms_providers``. If no keys are present in the
      # BSON input, the KMS provider settings configured for the mongocrypt_t
      # at initialization are used.
      #
      # @param [ Mongo::Crypt::Context ] context Encryption context.
      # @param [ BSON::Document ] kms_providers BSON document mapping
      #   the KMS provider names to credentials.
      #
      # @raise [ Mongo::Error::CryptError ] If the option is not set successfully.
      def self.ctx_provide_kms_providers(context, kms_providers)
        validate_document(kms_providers)
        data = kms_providers.to_bson.to_s
        Binary.wrap_string(data) do |data_p|
          check_ctx_status(context) do
            mongocrypt_ctx_provide_kms_providers(context.ctx_p, data_p)
          end
        end
      end

      # @!method self.mongocrypt_ctx_setopt_query_type(ctx, mongocrypt_query_type)
      #   @api private
      #
      # Set the query type to use for FLE 2 explicit encryption.
      # The query type is only used for indexed FLE 2 encryption.
      #
      # @param [ FFI::Pointer ] ctx A pointer to a mongocrypt_ctx_t object.
      # @param [ String ] query_type Type of the query.
      # @param [ Integer ] len The length of the query type string.
      #
      # @return [ Boolean ] Whether setting this option succeeded.
      attach_function(
        :mongocrypt_ctx_setopt_query_type,
        [
          :pointer,
          :string,
          :int
        ],
        :bool
      )

      # Set the query type to use for FLE 2 explicit encryption.
      # The query type is only used for indexed FLE 2 encryption.
      #
      # @param [ Mongo::Crypt::Context ] context Explicit encryption context.
      # @param [ String ] :mongocrypt_query_type query_type Type of the query.
      #
      # @raise [ Mongo::Error::CryptError ] If the operation failed.
      def self.ctx_setopt_query_type(context, query_type)
        check_ctx_status(context) do
          mongocrypt_ctx_setopt_query_type(context.ctx_p, query_type, -1)
        end
      end

      # @!method self.mongocrypt_ctx_setopt_contention_factor(ctx, contention_factor)
      #   @api private
      #
      # Set the contention factor used for explicit encryption.
      # The contention factor is only used for indexed FLE 2 encryption.
      #
      # @param [ FFI::Pointer ] ctx A pointer to a mongocrypt_ctx_t object.
      # @param [ int64 ] contention_factor
      #
      # @return [ Boolean ] Whether setting this option succeeded.
      attach_function(
        :mongocrypt_ctx_setopt_contention_factor,
        [
          :pointer,
          :int64
        ],
        :bool
      )

      # Set the contention factor used for explicit encryption.
      # The contention factor is only used for indexed FLE 2 encryption.
      #
      # @param [ Mongo::Crypt::Context ] context Explicit encryption context.
      # @param [ Integer ] factor Contention factor used for explicit encryption.
      #
      # @raise [ Mongo::Error::CryptError ] If the operation failed.
      def self.ctx_setopt_contention_factor(context, factor)
        check_ctx_status(context) do
          mongocrypt_ctx_setopt_contention_factor(context.ctx_p, factor)
        end
      end

      # @!method self.mongocrypt_ctx_setopt_algorithm_range(ctx, opts)
      #   @api private
      #
      # Set options for explicit encryption with the "rangePreview" algorithm.
      #
      # @note The RangePreview algorithm is experimental only. It is not intended for
      # public use.
      #
      # @param [ FFI::Pointer ] ctx A pointer to a mongocrypt_ctx_t object.
      # @param [ FFI::Pointer ] opts opts A pointer to range
      #   options document.
      #
      # @return [ Boolean ] Whether setting this option succeeded.
      attach_function(
        :mongocrypt_ctx_setopt_algorithm_range,
        [
          :pointer,
          :pointer
        ],
        :bool
      )

      # Set options for explicit encryption with the "rangePreview" algorithm.
      #
      # @note The RangePreview algorithm is experimental only. It is not intended for
      # public use.
      #
      # @param [ Mongo::Crypt::Context ] context
      # @param [ Hash ] opts options
      #
      # @raise [ Mongo::Error::CryptError ] If the operation failed
      def self.ctx_setopt_algorithm_range(context, opts)
        validate_document(opts)
        data = opts.to_bson.to_s
        Binary.wrap_string(data) do |data_p|
          check_ctx_status(context) do
            mongocrypt_ctx_setopt_algorithm_range(context.ctx_p, data_p)
          end
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

      # Checks that the specified data is a Hash before serializing
      # it to BSON to prevent errors from libmongocrypt
      #
      # @note All BSON::Document instances are also Hash instances
      #
      # @param [ Object ] data The data to be passed to libmongocrypt
      #
      # @raise [ Mongo::Error::CryptError ] If the data is not a Hash
      def self.validate_document(data)
        return if data.is_a?(Hash)

        if data.nil?
          message = "Attempted to pass nil data to libmongocrypt. " +
            "Data must be a Hash"
        else
          message = "Attempted to pass invalid data to libmongocrypt: #{data} " +
            "Data must be a Hash"
        end

        raise Error::CryptError.new(message)
      end
    end
  end
end

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

      # @!method self.mongocrypt_version(len)
      #   @api private
      #
      #   Returns the version string of the libmongocrypt library.
      #   @param [ FFI::Pointer | nil ] len (out param) An optional pointer to a
      #     uint8 that will reference the length of the returned string.
      #   @return [ String ] A version string for libmongocrypt.
      attach_function :mongocrypt_version, [:pointer], :string

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

      # @!method self.mongocrypt_setopt_kms_provider_aws(crypt, aws_access_key_id, aws_access_key_id_len, aws_secret_access_key, aws_secret_access_key_len)
      #   @api private
      #
      #   Configure mongocrypt_t object with AWS KMS provider options.
      #   @param [ FFI::Pointer ] crypt A pointer to a mongocrypt_t object.
      #   @param [ String ] aws_access_key_id The AWS access key id.
      #   @param [ Integer ] aws_access_key_id_len The length of the AWS access
      #     key string (or -1 for a null-terminated string).
      #   @param [ String ] aws_secret_access_key The AWS secret access key.
      #   @param [ Integer ] aws_secret_access_key_len The length of the AWS
      #     secret access key (or -1 for a null-terminated string).
      #   @return [ Boolean ] Returns whether the option was set successfully.
      attach_function(
        :mongocrypt_setopt_kms_provider_aws,
        [:pointer, :string, :int, :string, :int],
        :bool
      )

      # Configure the Handle object with AWS KMS provider options
      #
      # @param [ Mongo::Crypt::Handle ] handle
      # @param [ String ] aws_access_key The AWS access key
      # @param [ String ] aws_secret_access_key The AWS secret access key
      #
      # @raise [ Mongo::Error::CryptError ] If the option is not set successfully
      def self.setopt_kms_provider_aws(handle,
        aws_access_key, aws_secret_access_key
      )
        check_status(handle) do
          mongocrypt_setopt_kms_provider_aws(
            handle.ref,
            aws_access_key,
            -1,
            aws_secret_access_key,
            -1
          )
        end
      end

      # @!method self.mongocrypt_setopt_kms_provider_local(crypt, key)
      #   @api private
      #
      #   Configure mongocrypt_t object to take local KSM provider options.
      #   @param [ FFI::Pointer ] crypt A pointer to a mongocrypt_t object.
      #   @param [ FFI::Pointer ] key A pointer to a mongocrypt_binary_t object
      #     that references the 96-byte local master key.
      #   @return [ Boolean ] Returns whether the option was set successfully.
      attach_function(
        :mongocrypt_setopt_kms_provider_local,
        [:pointer, :pointer],
        :bool
      )

      # Set local KMS provider options on the Mongo::Crypt::Handle object
      #
      # @param [ Mongo::Crypt::Handle ] handle
      # @param [ String ] master_key The 96-byte local KMS master key
      #
      # @raise [ Mongo::Error::CryptError ] If the option is not set successfully
      def self.setopt_kms_provider_local(handle, master_key)
        Binary.wrap_string(master_key) do |master_key_p|
          check_status(handle) do
            mongocrypt_setopt_kms_provider_local(handle.ref, master_key_p)
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

      # @!method self.mongocrypt_ctx_setopt_masterkey_aws(ctx, region, region_len, arn, arn_len)
      #   @api private
      #
      #   Configure the ctx to take a master key from AWS.
      #   @param [ FFI::Pointer ] ctx A pointer to a mongocrypt_ctx_object.
      #   @param [ String ] region The AWS region.
      #   @param [ Integer ] region_len The length of the region string (or -1
      #     for a null-terminated string).
      #   @param [ String ] arn The Amazon Resource Name (ARN) of the mater key.
      #   @param [ Integer ] arn_len The length of the ARN (or -1 for a
      #     null-terminated string).
      #   @return [ Boolean ] Returns whether the option was set successfully.
      attach_function(
        :mongocrypt_ctx_setopt_masterkey_aws,
        [:pointer, :string, :int, :string, :int],
        :bool
      )

      # Configure the Context object to take a master key from AWS
      #
      # @param [ Mongo::Crypt::Context ] context
      # @param [ String ] region The AWS region (e.g. "us-east-2")
      # @param [ String ] arn The master key Amazon Resource Name
      #
      # @raise [ Mongo::Error::CryptError ] If the operation failed
      def self.ctx_setopt_master_key_aws(context, region, arn)
        check_ctx_status(context) do
          mongocrypt_ctx_setopt_masterkey_aws(
            context.ctx_p,
            region,
            -1,
            arn,
            -1
          )
        end
      end

      # @!method self.mongocrypt_ctx_setopt_masterkey_aws_endpoint(ctx, endpoint, endpoint_len)
      #   @api private
      #
      #   Set a custom endpoint at which to fetch the AWS master key
      #   @param [ FFI::Pointer ] ctx
      #   @param [ String ] endpoint The custom endpoint.
      #   @param [ Integer ] endpoint_len The length of the endpoint string (or
      #     -1 for a null-terminated string).
      #   @return [ Boolean ] Returns whether the option was set successfully.
      attach_function(
        :mongocrypt_ctx_setopt_masterkey_aws_endpoint,
        [:pointer, :string, :int],
        :bool
      )

      # Configure the Context object to take a masterk ey from AWS
      #
      # @param [ Mongo::Crypt::Context ] context
      # @param [ String ] endpoint The custom AWS master key endpoint
      #
      # @raise [ Mongo::Error::CryptError ] If the operation failed
      def self.ctx_setopt_master_key_aws_endpoint(context, endpoint)
        check_ctx_status(context) do
          mongocrypt_ctx_setopt_masterkey_aws_endpoint(
            context.ctx_p,
            endpoint,
            -1,
          )
        end
      end

      # @!method self.mongocrypt_ctx_setopt_masterkey_local(ctx)
      #   @api private
      #
      #   Set the ctx to take a local master key.
      #   @param [ FFI::Pointer ] ctx A pointer to a mongocrypt_ctx_t object.
      #   @note Do not initialize ctx before calling this method.
      #   @return [ Boolean ] Whether the option was successfully set.
      attach_function(
        :mongocrypt_ctx_setopt_masterkey_local,
        [:pointer],
        :bool
      )

      # Tell the Context object to read the master key from local KMS options
      #
      # @param [ Mongo::Crypt::Context ] context
      #
      # @raise [ Mongo::Error::CryptError ] If the operation failed
      def self.ctx_setopt_master_key_local(context)
        check_ctx_status(context) do
          mongocrypt_ctx_setopt_masterkey_local(context.ctx_p)
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
        :error,               0,
        :need_mongo_collinfo, 1,
        :need_mongo_markings, 2,
        :need_mongo_keys,     3,
        :need_kms,            4,
        :ready,               5,
        :done,                6,
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
          status.raise_crypt_error
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

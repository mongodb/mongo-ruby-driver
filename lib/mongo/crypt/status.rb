# frozen_string_literal: true
# rubocop:todo all

## Copyright (C) 2019-2020 MongoDB Inc.
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

    # A wrapper around mongocrypt_status_t, representing the status of
    # a mongocrypt_t handle.
    #
    # @api private
    class Status
      # Create a new Status object
      #
      # @param [ FFI::Pointer | nil ] pointer A pointer to an existing
      #   mongocrypt_status_t object. Defaults to nil.
      #
      # @note When initializing a Status object with a pointer, it is
      # recommended that you use the #self.from_pointer method
      def initialize(pointer: nil)
        # If a pointer is passed in, this class is not responsible for
        # destroying that pointer and deallocating data.
        #
        # FFI::AutoPointer uses a custom release strategy to automatically free
        # the pointer once this object goes out of scope
        @status = pointer || FFI::AutoPointer.new(
                              Binding.mongocrypt_status_new,
                              Binding.method(:mongocrypt_status_destroy)
                            )
      end

      # Initialize a Status object from an existing pointer to a
      # mongocrypt_status_t object.
      #
      # @param [ FFI::Pointer ] pointer A pointer to an existing
      #   mongocrypt_status_t object
      #
      # @return [ Mongo::Crypt::Status ] A new Status object
      def self.from_pointer(pointer)
        self.new(pointer: pointer)
      end

      # Set a label, code, and message on the Status
      #
      # @param [ Symbol ] label One of :ok, :error_client, or :error_kms
      # @param [ Integer ] code
      # @param [ String ] message
      #
      # @return [ Mongo::Crypt::Status ] returns self
      def update(label, code, message)
        unless [:ok, :error_client, :error_kms].include?(label)
          raise ArgumentError.new(
            "#{label} is an invalid value for a Mongo::Crypt::Status label. " +
            "Label must have one of the following values: :ok, :error_client, :error_kms"
          )
        end

        message_length = message ? message.bytesize + 1 : 0
        Binding.mongocrypt_status_set(@status, label, code, message, message_length)

        self
      end

      # Return the label of the status
      #
      # @return [ Symbol ] The status label, either :ok, :error_kms, or :error_client,
      #   defaults to :ok
      def label
        Binding.mongocrypt_status_type(@status)
      end

      # Return the integer code associated with the status
      #
      # @return [ Integer ] The status code, defaults to 0
      def code
        Binding.mongocrypt_status_code(@status)
      end

      # Return the status message
      #
      # @return [ String ] The status message, defaults to empty string
      def message
        message = Binding.mongocrypt_status_message(@status, nil)
        message || ''
      end

      # Checks whether the status is labeled :ok
      #
      # @return [ Boolean ] Whether the status is :ok
      def ok?
        Binding.mongocrypt_status_ok(@status)
      end

      # Returns the reference to the underlying mongocrypt_status_t
      # object
      #
      # @return [ FFI::Pointer ] Pointer to the underlying mongocrypt_status_t oject
      def ref
        @status
      end

      # Raises a Mongo::Error:CryptError corresponding to the
      # information stored in this status
      #
      # Does nothing if self.ok? is true
      #
      # @param kms [ true | false ] Whether the operation was against the KMS.
      #
      # @note If kms parameter is false, the error may still have come from a
      #   KMS. The kms parameter simply forces all errors to be treated as
      #   KMS errors.
      def raise_crypt_error(kms: false)
        return if ok?

        if kms || label == :error_kms
          error = Error::KmsError.new(message, code: code)
        else
          error = Error::CryptError.new(message, code: code)
        end

        raise error
      end
    end
  end
end

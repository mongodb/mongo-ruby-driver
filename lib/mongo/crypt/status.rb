## Copyright (C) 2019 MongoDB, Inc.
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
    # @since 2.12.0
    class Status
      # Create a new Status object
      #
      # @since 2.12.0
      def initialize
        @status = Binding.mongocrypt_status_new
      end

      # Set a label, code, and message on the Status
      #
      # @param [ Symbol ] label One of :ok, :error_client, or :error_kms
      # @param [ Integer ] code
      # @param [ String ] message
      #
      # @return [ Status ] returns self
      #
      # @since 2.12.0
      def set(label, code, message)
        unless [:ok, :error_client, :error_kms].include?(label)
          raise ArgumentError.new(
            "#{label} is an invalid value for a Mongo::Crypt::Status label. " +
            "Label must have one of the following values: :ok, :error_client, :error_kms"
          )
        end

        message_length = message ? message.length + 1 : 0
        Binding.mongocrypt_status_set(@status, label, code, message, message_length)

        self
      end

      # Return the label of the status
      #
      # @return [ Symbol ] The status label, either :ok, :error_kms, or :error_client,
      #   defaults to :ok
      #
      # @since 2.12.0
      def label
        Binding.mongocrypt_status_type(@status)
      end

      # Return the integer code associated with the status
      #
      # @return [ Integer ] The status code, defaults to 0
      #
      # @since 2.12.0
      def code
        Binding.mongocrypt_status_code(@status)
      end

      # Return the status message
      #
      # @return [ String ] The status message, defaults to empty string
      #
      # @since 2.12.0
      def message
        message = Binding.mongocrypt_status_message(@status, nil)
        message || ''
      end

      # Checks whether the status is labeled :ok
      #
      # @return [ Boolean ] Whether the status is :ok
      #
      # @since 2.12.0
      def ok?
        Binding.mongocrypt_status_ok(@status)
      end

      # Destroys reference to mongocrypt_status_t object and
      # cleans up resources.
      #
      # @return [ true ] Always true
      #
      # @since 2.12.0
      def close
        Binding.mongocrypt_status_destroy(@status)
        @status = nil

        true
      end

      # Convenient API for using status object without having
      # to perform cleanup.
      #
      # @example
      #   Mongo::Crypt::Status.with_status do |status|
      #     status.ok? # => true
      #   end
      #
      # @since 2.12.0
      def self.with_status
        status = self.new
        begin
          yield(status)
        ensure
          status.close
        end
      end
    end
  end
end

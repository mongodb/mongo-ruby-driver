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

module Mongo
  module Crypt

    # A Context object initialized specifically for the purpose of creating
    # a data key in the key managemenet system.
    #
    # @api private
    class DataKeyContext < Context

      # Create a new DataKeyContext object
      #
      # @param [ Mongo::Crypt::Handle ] mongocrypt a Handle that
      #   wraps a mongocrypt_t object used to create a new mongocrypt_ctx_t
      # @param [ Mongo::Crypt::EncryptionIO ] io An object that performs all
      #   driver I/O on behalf of libmongocrypt
      # @param [ String ] kms_provider The KMS provider to use. Options are
      #   "aws" and "local".
      # @param [ Hash ] options Data key creation options.
      #
      # @option [ Hash ] :masterkey A Hash of options related to the AWS
      #   KMS provider option. Required if kms_provider is "aws".
      #   - :region [ String ] The The AWS region of the master key (required).
      #   - :key [ String ] The Amazon Resource Name (ARN) of the master key (required).
      #   - :endpoint [ String ] An alternate host to send KMS requests to (optional).
      def initialize(mongocrypt, io, kms_provider, options={})
        unless ['aws', 'local'].include?(kms_provider)
          raise ArgumentError.new(
            "#{kms_provider} is an invalid kms provider. " +
            "Valid options are 'aws' and 'local'"
          )
        end

        super(mongocrypt, io)

        Binding.ctx_setopt_masterkey_local(self) if kms_provider == 'local'

        if kms_provider == 'aws'
          masterkey_opts = options[:masterkey]

          set_aws_master_key(masterkey_opts)
          set_aws_endpoint(masterkey_opts[:endpoint]) if masterkey_opts[:endpoint]
        end

        initialize_ctx
      end

      private

      # Configure the underlying mongocrypt_ctx_t object to accept AWS
      # KMS options
      def set_aws_master_key(masterkey_opts)
        unless masterkey_opts
          raise ArgumentError.new('The masterkey options cannot be nil')
        end

        unless masterkey_opts.is_a?(Hash)
          raise ArgumentError.new(
            "#{masterkey_opts} is an invalid masterkey option. " +
            "The masterkey option must be a Hash in the format " +
            "{ region: 'AWS-REGION', key: 'AWS-KEY-ARN' }"
          )
        end

        region = masterkey_opts[:region]
        unless region
          raise ArgumentError.new(
            'The :region key of the :masterkey options Hash cannot be nil'
          )
        end

        unless region.is_a?(String)
          raise ArgumentError.new(
            "#{masterkey_opts[:region]} is an invalid AWS masterkey region. " +
            "The :region key of the :masterkey options Hash must be a String"
          )
        end

        key = masterkey_opts[:key]
        unless key
          raise ArgumentError.new(
            'The :key key of the :masterkey options Hash cannot be nil'
          )
        end

        unless key.is_a?(String)
          raise ArgumentError.new(
            "#{masterkey_opts[:key]} is an invalid AWS masterkey key. " +
            "The :key key of the :masterkey options Hash must be a String"
          )
        end

        Binding.ctx_setopt_masterkey_aws(
          self,
          masterkey_opts[:region],
          masterkey_opts[:key],
        )
      end

      def set_aws_endpoint(endpoint)
        unless endpoint.is_a?(String)
          raise ArgumentError.new(
            "#{endpoint} is an invalid AWS masterkey endpoint. " +
            "The masterkey endpoint option must be a String"
          )
        end

        Binding.ctx_setopt_masterkey_aws_endpoint(self, endpoint)
      end

      # Initializes the underlying mongocrypt_ctx_t object
      def initialize_ctx
        Binding.ctx_datakey_init(self)
      end
    end
  end
end

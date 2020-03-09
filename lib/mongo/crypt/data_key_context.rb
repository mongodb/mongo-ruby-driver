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
      # @option options [ Hash ] :master_key A Hash of options related to the AWS
      #   KMS provider option. Required if kms_provider is "aws".
      #   - :region [ String ] The The AWS region of the master key (required).
      #   - :key [ String ] The Amazon Resource Name (ARN) of the master key (required).
      #   - :endpoint [ String ] An alternate host to send KMS requests to (optional).
      # @option options [ Array<String> ] :key_alt_names An optional array of strings specifying
      #   alternate names for the new data key.
      def initialize(mongocrypt, io, kms_provider, options={})
        super(mongocrypt, io)

        case kms_provider
        when 'local'
          Binding.ctx_setopt_master_key_local(self)
        when 'aws'
          unless options
            raise ArgumentError.new(
              'When "aws" is specified as the KMS provider, options cannot be nil'
            )
          end

          unless options.key?(:master_key)
            raise ArgumentError.new(
              'When "aws" is specified as the KMS provider, the options Hash ' +
              'must contain a key named :master_key with a Hash value in the ' +
              '{ region: "AWS-REGION", key: "AWS-KEY-ARN" }'
            )
          end

          master_key_opts = options[:master_key]

          set_aws_master_key(master_key_opts)
          set_aws_endpoint(master_key_opts[:endpoint]) if master_key_opts[:endpoint]
        else
          raise ArgumentError.new(
            "#{kms_provider} is an invalid kms provider. " +
            "Valid options are 'aws' and 'local'"
          )
        end

        set_key_alt_names(options[:key_alt_names]) if options[:key_alt_names]
        initialize_ctx
      end

      private

      # Configure the underlying mongocrypt_ctx_t object to accept AWS
      # KMS options
      def set_aws_master_key(master_key_opts)
        unless master_key_opts
          raise ArgumentError.new('The :master_key option cannot be nil')
        end

        unless master_key_opts.is_a?(Hash)
          raise ArgumentError.new(
            "#{master_key_opts} is an invalid :master_key option. " +
            "The :master_key option must be a Hash in the format " +
            "{ region: 'AWS-REGION', key: 'AWS-KEY-ARN' }"
          )
        end

        region = master_key_opts[:region]
        unless region
          raise ArgumentError.new(
            'The value of :region option of the :master_key options hash cannot be nil'
          )
        end

        unless region.is_a?(String)
          raise ArgumentError.new(
            "#{master_key_opts[:region]} is an invalid AWS master_key region. " +
            "The value of :region option of the :master_key options hash must be a String"
          )
        end

        key = master_key_opts[:key]
        unless key
          raise ArgumentError.new(
            'The value of :key option of the :master_key options hash cannot be nil'
          )
        end

        unless key.is_a?(String)
          raise ArgumentError.new(
            "#{master_key_opts[:key]} is an invalid AWS master_key key. " +
            "The value of :key option of the :master_key options hash must be a String"
          )
        end

        Binding.ctx_setopt_master_key_aws(
          self,
          region,
          key,
        )
      end

      def set_aws_endpoint(endpoint)
        unless endpoint.is_a?(String)
          raise ArgumentError.new(
            "#{endpoint} is an invalid AWS master_key endpoint. " +
            "The value of :endpoint option of the :master_key options hash must be a String"
          )
        end

        Binding.ctx_setopt_master_key_aws_endpoint(self, endpoint)
      end

      # Set the alt names option on the context
      def set_key_alt_names(key_alt_names)
        unless key_alt_names.is_a?(Array)
          raise ArgumentError.new, 'The :key_alt_names option must be an Array'
        end

        unless key_alt_names.all? { |key_alt_name| key_alt_name.is_a?(String) }
          raise ArgumentError.new(
            "#{key_alt_names} contains an invalid alternate key name. All " +
            "values of the :key_alt_names option Array must be Strings"
          )
        end

        Binding.ctx_setopt_key_alt_names(self, key_alt_names)
      end

      # Initializes the underlying mongocrypt_ctx_t object
      def initialize_ctx
        Binding.ctx_datakey_init(self)
      end
    end
  end
end

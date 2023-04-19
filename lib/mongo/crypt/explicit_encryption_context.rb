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

module Mongo
  module Crypt

    # A Context object initialized for explicit encryption
    #
    # @api private
    class ExplicitEncryptionContext < Context

      # Create a new ExplicitEncryptionContext object
      #
      # @param [ Mongo::Crypt::Handle ] mongocrypt a Handle that
      #   wraps a mongocrypt_t object used to create a new mongocrypt_ctx_t
      # @param [ ClientEncryption::IO ] io A instance of the IO class
      #   that implements driver I/O methods required to run the
      #   state machine
      # @param [ BSON::Document ] doc A document to encrypt
      #
      # @param [ Hash ] options
      # @option options [ BSON::Binary ] :key_id A BSON::Binary object of type
      #   :uuid representing the UUID of the data key to use for encryption.
      # @option options [ String ] :key_alt_name The alternate name of the data key
      #   that will be used to encrypt the value.
      # @option options [ String ] :algorithm The algorithm used to encrypt the
      #   value. Valid algorithms are "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic",
      #   "AEAD_AES_256_CBC_HMAC_SHA_512-Random", "Indexed", "Unindexed", "RangePreview".
      # @option options [ Integer | nil ] :contention_factor Contention factor
      #   to be applied if encryption algorithm is set to "Indexed". If not
      #   provided, it defaults to a value of 0. Contention factor should be set
      #   only if encryption algorithm is set to "Indexed".
      # @option options [ String | nil ] query_type Query type to be applied
      #   if encryption algorithm is set to "Indexed" or "RangePreview".
      #   Allowed values are "equality" and "rangePreview".
      # @option options [ Hash | nil ] :range_opts Specifies index options for
      #   a Queryable Encryption field supporting "rangePreview" queries.
      #   Allowed options are:
      #   - :min
      #   - :max
      #   - :sparsity
      #   - :precision
      #   min, max, sparsity, and range must match the values set in
      #   the encryptedFields of the destination collection.
      #   For double and decimal128, min/max/precision must all be set,
      #   or all be unset.
      #
      # @note The RangePreview algorithm is experimental only. It is not intended for
      # public use.
      #
      # @raise [ ArgumentError|Mongo::Error::CryptError ] If invalid options are provided
      def initialize(mongocrypt, io, doc, options = {})
        super(mongocrypt, io)
        set_key_opts(options)
        set_algorithm_opts(options)
        init(doc)
      end

      def init(doc)
        Binding.ctx_explicit_encrypt_init(self, doc)
      end

      private
      def set_key_opts(options)
        if options[:key_id].nil? && options[:key_alt_name].nil?
          raise ArgumentError.new(
            'The :key_id and :key_alt_name options cannot both be nil. ' +
            'Specify a :key_id option or :key_alt_name option (but not both)'
          )
        end
        if options[:key_id] && options[:key_alt_name]
          raise ArgumentError.new(
            'The :key_id and :key_alt_name options cannot both be present. ' +
            'Identify the data key by specifying its id with the :key_id ' +
            'option or specifying its alternate name with the :key_alt_name option'
          )
        end
        if options[:key_id]
          set_key_id(options[:key_id])
        elsif options[:key_alt_name]
          set_key_alt_name(options[:key_alt_name])
        end
      end

      def set_key_id(key_id)
        unless key_id.is_a?(BSON::Binary) &&
            key_id.type == :uuid
              raise ArgumentError.new(
                "Expected the :key_id option to be a BSON::Binary object with " +
                "type :uuid. #{key_id} is an invalid :key_id option"
              )
          end
          Binding.ctx_setopt_key_id(self, key_id.data)
      end

      def set_key_alt_name(key_alt_name)
        unless key_alt_name.is_a?(String)
            raise ArgumentError.new(':key_alt_name option must be a String')
          end
          Binding.ctx_setopt_key_alt_names(self, [key_alt_name])
      end

      def set_algorithm_opts(options)
        Binding.ctx_setopt_algorithm(self, options[:algorithm])
        if %w(Indexed RangePreview).include?(options[:algorithm])
          if options[:contention_factor]
            Binding.ctx_setopt_contention_factor(self, options[:contention_factor])
          end
          if options[:query_type]
            Binding.ctx_setopt_query_type(self, options[:query_type])
          end
        else
          if options[:contention_factor]
            raise ArgumentError.new(':contention_factor is allowed only for "Indexed" or "RangePreview" algorithms')
          end
          if options[:query_type]
            raise ArgumentError.new(':query_type is allowed only for "Indexed" or "RangePreview" algorithms')
          end
        end
        if options[:algorithm] == 'RangePreview'
          Binding.ctx_setopt_algorithm_range(self, convert_range_opts(options[:range_opts]))
        end
      end

      def convert_range_opts(range_opts)
        range_opts.dup.tap do |opts|
          opts[:sparsity] = BSON::Int64.new(opts[:sparsity]) unless opts[:sparsity].is_a?(BSON::Int64)
        end
      end
    end
  end
end

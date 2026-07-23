# frozen_string_literal: true

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
      #   "AEAD_AES_256_CBC_HMAC_SHA_512-Random", "Indexed", "Unindexed", "Range",
      #   "String".
      # @option options [ Integer | nil ] :contention_factor Contention factor
      #   to be applied if encryption algorithm is set to "Indexed". If not
      #   provided, it defaults to a value of 0. Contention factor should be set
      #   only if encryption algorithm is set to "Indexed".
      # @option options [ String | nil ] query_type Query type to be applied
      #   if encryption algorithm is set to "Indexed", "Range", or "String".
      #   Allowed values are "equality", "range", "prefix", "suffix", and
      #   "substring".
      # @option options [ Hash | nil ] :range_opts Specifies index options for
      #   a Queryable Encryption field supporting "range" queries.
      #   Allowed options are:
      #   - :min
      #   - :max
      #   - :trim_factor
      #   - :sparsity
      #   - :precision
      #   min, max, trim_factor, sparsity, and precision must match the values set in
      #   the encryptedFields of the destination collection.
      #   For double and decimal128, min/max/precision must all be set,
      #   or all be unset.
      # @option options [ Hash | nil ] :string_opts Specifies index options for
      #   a Queryable Encryption field supporting "prefix", "suffix", or
      #   "substring" queries (algorithm "String"). Allowed options are:
      #   - :case_sensitive
      #   - :diacritic_sensitive
      #   - :prefix (Hash with :str_min_query_length, :str_max_query_length)
      #   - :suffix (Hash with :str_min_query_length, :str_max_query_length)
      #   - :substring (Hash with :str_max_length, :str_min_query_length,
      #     :str_max_query_length)
      #   The options must match the values set in the encryptedFields of the
      #   destination collection.
      #
      # @note The Range algorithm is experimental only. It is not intended for
      # public use.
      # @note The "substring" query type is unstable and subject to backwards
      # breaking changes.
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
            'Expected the :key_id option to be a BSON::Binary object with ' +
            "type :uuid. #{key_id} is an invalid :key_id option"
          )
        end
        Binding.ctx_setopt_key_id(self, key_id.data)
      end

      def set_key_alt_name(key_alt_name)
        raise ArgumentError.new(':key_alt_name option must be a String') unless key_alt_name.is_a?(String)

        Binding.ctx_setopt_key_alt_names(self, [ key_alt_name ])
      end

      def set_algorithm_opts(options)
        Binding.ctx_setopt_algorithm(self, options[:algorithm])
        if %w[Indexed Range String].include?(options[:algorithm])
          Binding.ctx_setopt_contention_factor(self, options[:contention_factor]) if options[:contention_factor]
          Binding.ctx_setopt_query_type(self, options[:query_type]) if options[:query_type]
        else
          if options[:contention_factor]
            raise ArgumentError.new(':contention_factor is allowed only for "Indexed", "Range", or "String" algorithms')
          end
          if options[:query_type]
            raise ArgumentError.new(':query_type is allowed only for "Indexed", "Range", or "String" algorithms')
          end
        end
        if options[:algorithm] == 'Range'
          Binding.ctx_setopt_algorithm_range(self, convert_range_opts(options[:range_opts]))
        elsif options[:algorithm] == 'String'
          Binding.ctx_setopt_algorithm_text(self, convert_string_opts(options[:string_opts]))
        end
      end

      def convert_range_opts(range_opts)
        range_opts.dup.tap do |opts|
          opts[:sparsity] = BSON::Int64.new(opts[:sparsity]) if opts[:sparsity] && !opts[:sparsity].is_a?(BSON::Int64)
          opts[:trimFactor] = opts.delete(:trim_factor) if opts[:trim_factor]
        end
      end

      def convert_string_opts(string_opts)
        raise ArgumentError.new(':string_opts is required for the "String" algorithm') if string_opts.nil?

        string_opts.dup.tap do |opts|
          opts[:caseSensitive] = opts.delete(:case_sensitive) if opts.key?(:case_sensitive)
          opts[:diacriticSensitive] = opts.delete(:diacritic_sensitive) if opts.key?(:diacritic_sensitive)
          %i[substring prefix suffix].each do |query_type|
            opts[query_type] = convert_string_query_opts(opts[query_type]) if opts[query_type]
          end
        end
      end

      def convert_string_query_opts(query_opts)
        query_opts.dup.tap do |opts|
          opts[:strMaxLength] = opts.delete(:str_max_length) if opts.key?(:str_max_length)
          opts[:strMinQueryLength] = opts.delete(:str_min_query_length) if opts.key?(:str_min_query_length)
          opts[:strMaxQueryLength] = opts.delete(:str_max_query_length) if opts.key?(:str_max_query_length)
        end
      end
    end
  end
end

# frozen_string_literal: true
# encoding: utf-8

module Unified
  module ClientSideEncryptionOperations
    def create_key(op)
      client_encryption = entities.get(:clientEncryption, op.use!('object'))
      use_arguments(op) do |args|
        opts = Utils.shallow_snakeize_hash(args.use('opts')) || {}
        opts[:master_key] = Utils.shallow_snakeize_hash(opts[:master_key]) if opts[:master_key]
        client_encryption.create_key(
          args.use!('kmsProvider'),
          opts,
        )
      end
    end

    def add_key_alt_name(op)
      client_encryption = entities.get(:clientEncryption, op.use!('object'))
      use_arguments(op) do |args|
        client_encryption.add_key_alt_name(
          args.use!('id'),
          args.use!('keyAltName')
        )
      end
    end

    def delete_key(op)
      client_encryption = entities.get(:clientEncryption, op.use!('object'))
      use_arguments(op) do |args|
        client_encryption.delete_key(
          args.use!('id')
        )
      end
    end

    def get_key(op)
      client_encryption = entities.get(:clientEncryption, op.use!('object'))
      use_arguments(op) do |args|
        client_encryption.get_key(
          args.use!('id')
        )
      end
    end

    def get_key_by_alt_name(op)
      client_encryption = entities.get(:clientEncryption, op.use!('object'))
      use_arguments(op) do |args|
        client_encryption.get_key_by_alt_name(
          args.use!('keyAltName')
        )
      end
    end
  end
end

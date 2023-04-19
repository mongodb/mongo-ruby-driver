# frozen_string_literal: true
# rubocop:todo all

module Unified
  module ClientSideEncryptionOperations
    def create_data_key(op)
      client_encryption = entities.get(:clientEncryption, op.use!('object'))
      use_arguments(op) do |args|
        opts = Utils.shallow_snakeize_hash(args.use('opts')) || {}
        opts[:master_key] = Utils.shallow_snakeize_hash(opts[:master_key]) if opts[:master_key]
        opts[:key_material] = opts[:key_material].data if opts[:key_material]
        client_encryption.create_data_key(
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

    def get_keys(op)
      client_encryption = entities.get(:clientEncryption, op.use!('object'))
      client_encryption.get_keys.to_a
    end

    def remove_key_alt_name(op)
      client_encryption = entities.get(:clientEncryption, op.use!('object'))
      use_arguments(op) do |args|
        client_encryption.remove_key_alt_name(
          args.use!('id'),
          args.use!('keyAltName')
        )
      end
    end

    def rewrap_many_data_key(op)
      client_encryption = entities.get(:clientEncryption, op.use!('object'))
      use_arguments(op) do |args|
        opts = Utils.shallow_snakeize_hash(args.use('opts')) || {}
        opts[:master_key] = Utils.shallow_snakeize_hash(opts[:master_key]) if opts[:master_key]
        client_encryption.rewrap_many_data_key(
          args.use!('filter'),
          opts
        )
      end
    end
  end
end

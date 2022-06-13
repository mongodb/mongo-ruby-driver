# frozen_string_literal: true
# encoding: utf-8

module Unified
  module ClientSideEncryptionOperations
    def create_key(op)
      client_encryption = entities.get(:clientEncryption, op.use!('object'))
      use_arguments(op) do |args|
        opts = Utils.shallow_snakeize_hash(args.use('opts'))
        opts[:master_key] = Utils.shallow_snakeize_hash(opts[:master_key])
        client_encryption.create_key(
          args.use!('kmsProvider'),
          opts,
        )
      end
    end
  end
end

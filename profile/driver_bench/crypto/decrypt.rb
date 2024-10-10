# frozen_string_literal: true

require 'mongo'
require_relative '../base'

module Mongo
  module DriverBench
    module Crypto
      # Benchmark for reporting the performance of decrypting a document with
      # a large number of encrypted fields.
      class Decrypt < Mongo::DriverBench::Base
        ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic'
        KEY_VAULT_NAMESPACE = 'encryption.__keyVault'
        N = 10

        def run
          doc = build_encrypted_doc

          # warm up
          run_test(doc, 1)

          [ 1, 2, 8, 64 ].each do |thread_count|
            run_test_with_thread_count(doc, thread_count)
          end
        end

        private

        def run_test_with_thread_count(doc, thread_count)
          results = []

          N.times do
            threads = Array.new(thread_count) do
              Thread.new { Thread.current[:ops_sec] = run_test(doc, 1) }
            end

            results << threads.each(&:join).sum { |t| t[:ops_sec] }
          end

          median = results.sort[N / 2]
          puts "thread_count=#{thread_count}; median ops/sec=#{median}"
        end

        def build_encrypted_doc
          data_key_id = client_encryption.create_data_key('local')

          pairs = Array.new(1500) do |i|
            n = format('%04d', i + 1)
            key = "key#{n}"
            value = "value #{n}"

            encrypted = client_encryption.encrypt(value,
                                                  key_id: data_key_id,
                                                  algorithm: ALGORITHM)

            [ key, encrypted ]
          end

          BSON::Document[pairs]
        end

        def timeout_holder
          @timeout_holder ||= Mongo::CsotTimeoutHolder.new
        end

        def encrypter
          @encrypter ||= Crypt::AutoEncrypter.new(
            client: new_client,
            key_vault_client: key_vault_client,
            key_vault_namespace: KEY_VAULT_NAMESPACE,
            kms_providers: kms_providers
          )
        end

        def run_test(doc, duration)
          finish_at = Mongo::Utils.monotonic_time + duration
          count = 0

          while Mongo::Utils.monotonic_time < finish_at
            encrypter.decrypt(doc, timeout_holder)
            count += 1
          end

          count
        end

        def key_vault_client
          @key_vault_client ||= new_client
        end

        def kms_providers
          @kms_providers ||= { local: { key: SecureRandom.random_bytes(96) } }
        end

        def client_encryption
          @client_encryption ||= Mongo::ClientEncryption.new(
            key_vault_client,
            key_vault_namespace: KEY_VAULT_NAMESPACE,
            kms_providers: kms_providers
          )
        end
      end
    end
  end
end

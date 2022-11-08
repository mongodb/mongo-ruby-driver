# frozen_string_literal: true
# encoding: utf-8

require 'spec_helper'

describe 'Do not connect to mongocryptd if shared library is loaded' do
  require_libmongocrypt
  require_enterprise

  include_context 'define shared FLE helpers'
  include_context 'with local kms_providers'

  let(:extra_options) do
    {
      crypt_shared_lib_path: SpecConfig.instance.crypt_shared_lib_path,
      mongocryptd_uri: "mongodb://localhost:27777"
    }
  end

  let(:encryption_client) do
    new_local_client(
      SpecConfig.instance.addresses,
      SpecConfig.instance.test_options.merge(
        auto_encryption_options: {
          kms_providers: kms_providers,
          kms_tls_options: kms_tls_options,
          key_vault_namespace: key_vault_namespace,
          schema_map: { "auto_encryption.users" => schema_map },
          extra_options: extra_options,
        },
        database: 'auto_encryption',
      ),
    )
  end

  let!(:connect_attempt) do
    Class.new do
      def lock
        @lock ||= Mutex.new
      end

      def done?
        lock.synchronize do
          !!@done
        end
      end

      def done!
        lock.synchronize do
          @done = true
        end
      end
    end.new
  end

  let!(:listener) do
    Thread.new do
      TCPServer.new(27777).accept
      connect_attempt.done!
    end
  end

  after do
    listener.exit
  end

  it 'does not try to connect to mongocryptd' do
    skip 'This test requires crypt shared library' unless SpecConfig.instance.crypt_shared_lib_path

    encryption_client[:users].insert_one(ssn: ssn)
    expect(connect_attempt.done?).to eq(false)
  end
end

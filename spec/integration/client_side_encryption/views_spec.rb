# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe 'Client-Side Encryption' do
  describe 'Prose tests: Data key and double encryption' do
    require_libmongocrypt
    require_enterprise
    min_server_fcv '4.2'

    include_context 'define shared FLE helpers'

    let(:client) do
      new_local_client(
        SpecConfig.instance.addresses,
        SpecConfig.instance.test_options
      )
    end

    let(:client_encrypted) do
      new_local_client(
        SpecConfig.instance.addresses,
        SpecConfig.instance.test_options.merge(
          auto_encryption_options: {
            kms_providers: local_kms_providers,
            key_vault_namespace: 'keyvault.datakeys',
            # Spawn mongocryptd on non-default port for sharded cluster tests
            extra_options: extra_options,
          },
          database: 'db',
        )
      )
    end

    before do
      client.use('db')['view'].drop
      client.use('db').database.command(create: 'view', viewOn: 'coll')
    end

    it 'does not perform encryption on views' do
      expect do
        client_encrypted['view'].insert_one({})
      end.to raise_error(Mongo::Error::CryptError, /cannot auto encrypt a view/)
    end
  end
end

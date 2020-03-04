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
        SpecConfig.instance.test_options.merge(database: :db)
      )
    end

    let(:client_encrypted) do
      new_local_client(
        SpecConfig.instance.addresses,
        SpecConfig.instance.test_options.merge(
          auto_encryption_options: {
            kms_providers: local_kms_providers,
            key_vault_namespace: 'admin.datakeys',
          },
          database: :db,
        )
      )
    end

    before do
      client.database.view.drop
      client.database.command(create: 'view', viewOn: 'coll')
    end

    it 'does not perform encryption on views' do
      expect do
        client_encrypted.database.view.insert_one({})
      end.to raise_error(/blah/)
    end
  end
end

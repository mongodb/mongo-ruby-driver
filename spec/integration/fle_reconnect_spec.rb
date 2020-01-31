require 'spec_helper'

describe 'Client with auto encryption #reconnect' do
  require_libmongocrypt
  require_enterprise

  let(:client) do
    new_local_client(
      SpecConfig.instance.addresses,
      {
        auto_encryption_options: {
          kms_providers: { local: { key: Base64.encode64("\x00" * 96) } },
          key_vault_namespace: 'admin.datakeys',
          key_vault_client: key_vault_client_option
        }
      }
    )
  end

  let(:mongocryptd_client) { client.mongocryptd_client }
  let(:key_vault_client) { client.key_vault_client }

  let(:json_schema) do
    BSON::ExtJSON.parse(File.read('spec/mongo/crypt/data/schema_map.json'))
  end

  before do
    client['test'].insert_one('testk' => 'testv')
    key_vault_client['datakeys'].insert_one('key_id' => 'key_material')
  end

  shared_examples 'a functioning client' do
    it 'can perform a find command' do
      doc = client['test'].find('testk' => 'testv').first
      expect(doc).not_to be_nil
      expect(doc['testk']).to eq('testv')
    end
  end

  shared_examples 'a functioning mongocryptd client' do
    before do
      client.spawn_mongocryptd
    end

    it 'can perform a schemaRequiresEncryption command' do
      # A schemaRequiresEncryption command; mongocryptd should respond that
      # this command requires encryption.
      response = mongocryptd_client.database.command(
        insert: 'users',
        ordered: true,
        lsid: { id: BSON::Binary.new("\x00" * 16, :uuid) },
        documents: [{
          ssn: '123-456-7890',
          _id: BSON::ObjectId.new,
        }],
        jsonSchema: json_schema,
        isRemoteSchema: false
      )

      expect(response).to be_ok
      expect(response.documents.first['schemaRequiresEncryption']).to be true
    end
  end

  shared_examples 'a functioning key vault client' do
    it 'can perform a find command' do
      doc = key_vault_client['datakeys'].find('key_id' => 'key_material').first
      expect(doc).not_to be_nil
      expect(doc['key_id']).to eq('key_material')
    end
  end

  shared_examples 'an auto-encryption client that reconnects properly' do
    context 'after reconnecting without closing main client' do
      before do
        client.reconnect
      end

      it_behaves_like 'a functioning client'
      it_behaves_like 'a functioning mongocryptd client'
      it_behaves_like 'a functioning key vault client'
    end

    context 'after closing and reconnecting main client' do
      before do
        client.close
        client.reconnect
      end

      it_behaves_like 'a functioning client'
      it_behaves_like 'a functioning mongocryptd client'
      it_behaves_like 'a functioning key vault client'
    end

    context 'after killing client monitor thread' do
      before do
        thread = client.cluster.servers.first.monitor.instance_variable_get('@thread')
        expect(thread).to be_alive

        thread.kill

        sleep 0.1
        expect(thread).not_to be_alive

        client.reconnect
      end

      it_behaves_like 'a functioning client'
      it_behaves_like 'a functioning mongocryptd client'
      it_behaves_like 'a functioning key vault client'
    end

    context 'after closing mongocryptd client and reconnecting' do
      before do
        mongocryptd_client.close
        client.reconnect
      end

      it_behaves_like 'a functioning client'
      it_behaves_like 'a functioning mongocryptd client'
      it_behaves_like 'a functioning key vault client'
    end

    context 'after killing mongocryptd client monitor thread and reconnecting' do
      before do
        thread = mongocryptd_client.cluster.servers.first.monitor.instance_variable_get('@thread')
        expect(thread).to be_alive

        thread.kill

        sleep 0.1
        expect(thread).not_to be_alive

        client.reconnect
      end

      it_behaves_like 'a functioning client'
      it_behaves_like 'a functioning mongocryptd client'
      it_behaves_like 'a functioning key vault client'
    end

    context 'after closing key_vault_client and reconnecting' do
      before do
        key_vault_client.close
        client.reconnect
      end

      it_behaves_like 'a functioning client'
      it_behaves_like 'a functioning mongocryptd client'
      it_behaves_like 'a functioning key vault client'
    end

    context 'after killing key_vault_client monitor thread and reconnecting' do
      before do
        thread = key_vault_client.cluster.servers.first.monitor.instance_variable_get('@thread')
        expect(thread).to be_alive

        thread.kill

        sleep 0.1
        expect(thread).not_to be_alive

        client.reconnect
      end

      it_behaves_like 'a functioning client'
      it_behaves_like 'a functioning mongocryptd client'
      it_behaves_like 'a functioning key vault client'
    end
  end

  context 'with default key vault client option' do
    let(:key_vault_client_option) { nil }

    it_behaves_like 'an auto-encryption client that reconnects properly'
  end

  context 'with custom key vault client option' do
    let(:key_vault_client_option) do
      Mongo::Client.new(SpecConfig.instance.addresses).use(:test)
    end

    it_behaves_like 'an auto-encryption client that reconnects properly'
  end
end

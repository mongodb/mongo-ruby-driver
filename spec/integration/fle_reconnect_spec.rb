require 'spec_helper'

describe 'Client with auto encryption after reconnect' do
  let(:client) do
    new_local_client(
      'mongodb://localhost:27017/test',
      {
        auto_encryption_options: {
          kms_providers: { local: { key: Base64.encode64('ruby' * 24) } },
          key_vault_namespace: 'admin.datakeys',
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
  end

  shared_examples 'a functioning client' do
    it 'can find the document' do
      doc = client['test'].find('testk' => 'testv').first
      expect(doc).not_to be_nil
      expect(doc['testk']).to eq('testv')
    end
  end

  shared_examples 'a functioning mongocryptd client' do
    it 'can perform a schemaRequiresEncryption command' do
      client.spawn_mongocryptd
      sleep 5
      response = mongocryptd_client.database.command(
        insert: 'users',
        ordered: true,
        lsid: { id: BSON::Binary.new("\v8#O\xE6\xF2D\xAF\x85)E\x86\xE9\x06\xF2\x8D", :uuid) },
        documents: [{
          ssn: '123-456-7890',
          _id: BSON::ObjectId('5e16516e781d8a89b94df6df'),
        }],
        jsonSchema: json_schema,
        isRemoteSchema: false
      )

      expect(response).to be_ok
      expect(response.documents.first['schemaRequiresEncryption']).to be true
    end
  end

  context 'after reconnecting without closing main client' do
    before do
      client.reconnect
    end

    it_behaves_like 'a functioning client'
    it_behaves_like 'a functioning mongocryptd client'
  end

  # context 'after closing and reconnecting main client' do
  #   before do
  #     client.close
  #     client.reconnect
  #   end

  #   it_behaves_like 'a functioning client'
  #   it_behaves_like 'a functioning mongocryptd client'
  # end

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
  end

  # context 'after closing and reconnecting mongocryptd client' do

  # end

  context 'after killing mongocryptd client monitor thread' do
    before do
      byebug
      thread = mongocryptd_client.cluster.servers.first.monitor.instance_variable_get('@thread')
      expect(thread).to be_alive

      thread.kill

      sleep 0.1
      expect(thread).not_to be_alive

      mongocryptd_client.reconnect
    end

    it_behaves_like 'a functioning client'
    it_behaves_like 'a functioning mongocryptd client'
  end

end

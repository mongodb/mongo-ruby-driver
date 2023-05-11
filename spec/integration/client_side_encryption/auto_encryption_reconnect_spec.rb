# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe 'Client with auto encryption #reconnect' do
  require_libmongocrypt
  min_server_fcv '4.2'
  require_enterprise

  # Diagnostics of leaked background threads only, these tests do not
  # actually require a clean slate. https://jira.mongodb.org/browse/RUBY-2138
  clean_slate

  include_context 'define shared FLE helpers'

  let(:client) do
    new_local_client(
      SpecConfig.instance.addresses,
      SpecConfig.instance.test_options.merge(
        {
          auto_encryption_options: {
            kms_providers: kms_providers,
            kms_tls_options: kms_tls_options,
            key_vault_namespace: key_vault_namespace,
            key_vault_client: key_vault_client_option,
            schema_map: { 'auto_encryption.users': schema_map },
            # Spawn mongocryptd on non-default port for sharded cluster tests
            extra_options: extra_options,
          },
          database: 'auto_encryption',
          populator_io: false
        }
      )
    )
  end

  let(:unencrypted_client) { authorized_client.use('auto_encryption') }

  let(:mongocryptd_client) { client.encrypter.mongocryptd_client }
  let(:key_vault_client) { client.encrypter.key_vault_client }
  let(:data_key_id) { data_key['_id'] }

  shared_examples 'a functioning client' do
    it 'can perform an encrypted find command' do
      doc = client['users'].find(ssn: ssn).first
      expect(doc).not_to be_nil
      expect(doc['ssn']).to eq(ssn)
    end
  end

  shared_examples 'a functioning mongocryptd client' do
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
        jsonSchema: schema_map,
        isRemoteSchema: false
      )

      expect(response).to be_ok
      expect(response.documents.first['schemaRequiresEncryption']).to be true
    end
  end

  shared_examples 'a functioning key vault client' do
    it 'can perform a find command' do
      doc = key_vault_client.use(key_vault_db)[key_vault_coll, read_concern: { level: :majority}].find(_id: data_key_id).first
      expect(doc).not_to be_nil
      expect(doc['_id']).to eq(data_key_id)
    end
  end

  shared_examples 'an auto-encryption client that reconnects properly' do
    before do
      key_vault_collection.drop
      key_vault_collection.insert_one(data_key)

      unencrypted_client['users'].drop
      # Use a client without auto_encryption_options to insert an
      # encrypted document into the collection; this ensures that the
      # client with auto_encryption_options must perform decryption
      # to properly read the document.
      unencrypted_client['users'].insert_one(
        ssn: BSON::Binary.new(Base64.decode64(encrypted_ssn), :ciphertext)
      )
    end

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
        # don't use the mongocryptd_client variable yet so that it will be computed
        # after the client reconnects
        client.encrypter.mongocryptd_client.close
        client.reconnect
      end

      it_behaves_like 'a functioning client'
      it_behaves_like 'a functioning mongocryptd client'
      it_behaves_like 'a functioning key vault client'
    end

    context 'after killing mongocryptd client monitor thread and reconnecting' do
      before do
        # don't use the mongocryptd_client variable yet so that it will be computed
        # after the client reconnects
        thread = client.encrypter.mongocryptd_client.cluster.servers.first.monitor.instance_variable_get('@thread')
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

    context 'with AWS KMS providers' do
      include_context 'with AWS kms_providers'
      it_behaves_like 'an auto-encryption client that reconnects properly'
    end

    context 'with Azure KMS providers' do
      include_context 'with Azure kms_providers'
      it_behaves_like 'an auto-encryption client that reconnects properly'
    end

    context 'with GCP KMS providers' do
      include_context 'with GCP kms_providers'
      it_behaves_like 'an auto-encryption client that reconnects properly'
    end

    context 'with KMIP KMS providers' do
      include_context 'with KMIP kms_providers'
      it_behaves_like 'an auto-encryption client that reconnects properly'
    end

    context 'with local KMS providers' do
      include_context 'with local kms_providers'
      it_behaves_like 'an auto-encryption client that reconnects properly'
    end
  end

  context 'with custom key vault client option' do
    let(:key_vault_client_option) do
      new_local_client(
        SpecConfig.instance.addresses,
        SpecConfig.instance.test_options.merge(populator_io: false)
      )
    end

    context 'with AWS KMS providers' do
      include_context 'with AWS kms_providers'
      it_behaves_like 'an auto-encryption client that reconnects properly'
    end

    context 'with Azure KMS providers' do
      include_context 'with Azure kms_providers'
      it_behaves_like 'an auto-encryption client that reconnects properly'
    end

    context 'with GCP KMS providers' do
      include_context 'with GCP kms_providers'
      it_behaves_like 'an auto-encryption client that reconnects properly'
    end

    context 'with KMIP KMS providers' do
      include_context 'with KMIP kms_providers'
      it_behaves_like 'an auto-encryption client that reconnects properly'
    end

    context 'with local KMS providers' do
      include_context 'with local kms_providers'
      it_behaves_like 'an auto-encryption client that reconnects properly'
    end
  end
end

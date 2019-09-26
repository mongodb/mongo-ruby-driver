# require 'spec_helper'
require 'lite_spec_helper'

describe 'Client authentication with MONGODB-X509' do
  before do
    skip 'X509_REQUIRED env var not specified' unless ENV['X509_REQUIRED']

    users = ClientRegistry.instance.global_client('root_authorized').use(:$external).database.users

    unless users.info(SpecConfig.instance.x509_username).empty?
      users.remove(SpecConfig.instance.x509_username)
    end
  end

  context 'with URI options' do
    let(:tls) { true }

    let(:uri) do
      "mongodb://#{SpecConfig.instance.addresses.first}/?" +
        "authMechanism=MONGODB-X509&tls=#{tls}" +
        "&tlsCAFile=#{SpecConfig.instance.local_ca_cert_path}" +
        "&tlsCertificateKeyFile=#{SpecConfig.instance.client_x509_pem_path}" +
        "&serverSelectionTimeoutMS=3000"
    end

    let(:client) do
      client = ClientRegistry.instance.new_local_client(uri)
    end

    context 'without tls option' do
      let(:tls) { false }

      it 'cannot detect server' do
        expect {
          client['client_construction'].insert_one(test: 1)
        }.to raise_error(Mongo::Error::NoServerAvailable)
      end
    end

    context 'without creating the corresponding user in $external' do
      it 'raises an unauthorized exception' do
        expect {
          client['client_construction'].insert_one(test: 1)
        }.to raise_error(Mongo::Auth::Unauthorized)
      end
    end

    context 'with the correct user' do
      before do
        users = ClientRegistry.instance.global_client('root_authorized').use(:$external).database.users
        users.create(SpecConfig.instance.x509_username)
      end

      it 'can connect to the server and insert a document' do
        result = client['client_construction'].insert_one(test: 1)
        expect(result).to be_ok
      end
    end
  end

  context 'with client options' do
    let(:options) do
      {
        auth_mech: :mongodb_x509,
        ssl: ssl,
        ssl_cert: SpecConfig.instance.client_x509_pem_path,
        ssl_ca_cert: SpecConfig.instance.local_ca_cert_path,
        server_selection_timeout: 3
      }
    end

    let(:ssl) { true }

    let(:client) do
      ClientRegistry.instance.new_local_client([SpecConfig.instance.addresses.first], options)
    end

    context 'without ssl option' do
      let(:ssl) { false }

      it 'cannot detect server' do
        expect {
          client['client_construction'].insert_one(test: 1)
        }.to raise_error(Mongo::Error::NoServerAvailable)
      end
    end

    context 'without creating the corresponding user in $external' do
      it 'raises an unauthorized exception' do
        expect {
          client['client_construction'].insert_one(test: 1)
        }.to raise_error(Mongo::Auth::Unauthorized)
      end
    end

    context 'with the correct user' do
      before do
        users = ClientRegistry.instance.global_client('root_authorized').use(:$external).database.users
        users.create(SpecConfig.instance.x509_username)
      end

      it 'can connect to the server and insert a document' do
        result = client['client_construction'].insert_one(test: 1)
        expect(result).to be_ok
      end
    end
  end
end

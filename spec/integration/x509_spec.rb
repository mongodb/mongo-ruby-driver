require 'spec_helper'

describe 'X509 client authentication' do
  before(:all) do
    user = 'C=US,ST=New York,L=New York City,O=MongoDB,OU=x509,CN=localhost'
    users = ClientRegistry.instance.global_client('root_authorized').use(:$external).database.users
    users.remove(user) unless users.info(user).empty?
  end

  context 'without the correct user' do
    it 'cannot insert a document' do
      client = ClientRegistry.instance.new_local_client("mongodb://localhost:27017/?authMechanism=MONGODB-X509&tls=true&tlsCAFile=spec/support/certificates/ca.crt&tlsCertificateKeyFile=spec/support/certificates/client-x509.pem", database: 'test')
      expect{
        client[:test].insert_one({ text: 'Hello, world!' })
    }.to raise_error(Mongo::Auth::Unauthorized)
    end
  end

  context 'with a database user in place' do
    before(:all) do
      user = 'C=US,ST=New York,L=New York City,O=MongoDB,OU=x509,CN=localhost'

      users = ClientRegistry.instance.global_client('root_authorized').use(:$external).database.users

      users.remove(user) unless users.info(user).empty?
      users.create(user)
    end

    context 'with client options' do
      it 'successfully inserts a document' do
        client = ClientRegistry.instance.new_local_client("mongodb://localhost:27017",
          {
            auth_mech: :mongodb_x509,
            ssl: true,
            ssl_cert: "spec/support/certificates/client-x509.pem",
            ssl_ca_cert: "spec/support/certificates/ca.crt",
            database: "test",
          }
        )

        result = client[:test].insert_one({ text: 'Hello, world!' })
        expect(result).to be_ok
      end
    end

    context 'with URI options' do
      it 'successfully inserts a document' do
        client = ClientRegistry.instance.new_local_client("mongodb://localhost:27017/?authMechanism=MONGODB-X509&tls=true&tlsCAFile=spec/support/certificates/ca.crt&tlsCertificateKeyFile=spec/support/certificates/client-x509.pem", database: 'test')
        result = client[:test].insert_one({ text: 'Hello, world!' })

        expect(result).to be_ok
      end
    end
  end
end

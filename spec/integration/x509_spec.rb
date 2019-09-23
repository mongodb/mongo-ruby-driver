require 'spec_helper'

describe 'X509 client authentication' do
  # before(:all) do
  #   # If auth is configured, the test suite uses the configured user
  #   # and does not create its own users. However, the configured user may
  #   # not have the auth mechanisms we need. Therefore we create a user
  #   # for this test without specifying auth mechanisms, which gets us
  #   # server default (scram for 4.0, scram & scram256 for 4.2).

  #   users = ClientRegistry.instance.global_client('root_authorized').use(:admin).database.users
  #   unless users.info('existing_user').empty?
  #     users.remove('existing_user')
  #   end
  #   users.create('existing_user', password: 'password')
  # end

  before(:all) do
    client = ClientRegistry.instance.global_client('root_authorized').use(:$external)
    users = client.database.users

    unless users.info('C=US,ST=New York,L=New York City,O=MongoDB,OU=x509,CN=localhost').empty?
      users.remove('C=US,ST=New York,L=New York City,O=MongoDB,OU=x509,CN=localhost')
    end

    users.create("C=US,ST=New York,L=New York City,O=MongoDB,OU=x509,CN=localhost")
  end

  context 'attempting to connect without tls options' do

  end

  context 'with URI options' do

  end

  context 'with client options' do
    it 'successfully inserts a document' do
      client = ClientRegistry.instance.new_local_client("mongodb://localhost:27017/?authMechanism=MONGODB-X509&tls=true&tlsCAFile=spec/support/certificates/ca.crt&tlsCertificateKeyFile=spec/support/certificates/client-x509.pem", database: 'test')
      result = client[:test].insert_one({ text: 'Hello, world!' })

      expect(result).to be_ok
    end
  end
end

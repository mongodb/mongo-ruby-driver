require 'spec_helper'

describe Mongo::Auth::X509 do

  let(:server) do
    authorized_client.cluster.next_primary
  end

  let(:connection) do
    Mongo::Server::Connection.new(server, SpecConfig.instance.test_options)
  end

  let(:user) do
    Mongo::Auth::User.new(database: SpecConfig.instance.test_db, user: 'driver', password: 'password')
  end

  describe '#login' do

    context 'when the user is not authorized for the database' do

      let(:x509) do
        described_class.new(user)
      end

      let(:login) do
        x509.login(connection).documents[0]
      end

      it 'logs the user into the connection' do
        expect {
          x509.login(connection)
        }.to raise_error(Mongo::Auth::Unauthorized)
      end
    end
  end
end

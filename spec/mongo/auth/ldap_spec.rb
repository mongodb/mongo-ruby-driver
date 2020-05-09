require 'spec_helper'

describe Mongo::Auth::LDAP do

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

    before do
      connection.connect!
    end

    context 'when the user is not authorized for the database' do

      let(:cr) do
        described_class.new(user, connection)
      end

      let(:login) do
        cr.login.documents[0]
      end

      it 'attempts to log the user into the connection' do
        expect {
          cr.login
        }.to raise_error(Mongo::Auth::Unauthorized)
      end
    end
  end
end

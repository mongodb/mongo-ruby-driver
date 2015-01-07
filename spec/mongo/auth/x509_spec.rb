require 'spec_helper'

describe Mongo::Auth::X509 do

  let(:server) do
    Mongo::Server.new('127.0.0.1:27017', Mongo::Event::Listeners.new)
  end

  let(:connection) do
    Mongo::Connection.new(server)
  end

  let(:user) do
    Mongo::Auth::User.new(database: TEST_DB, user: 'driver', password: 'password')
  end

  describe '#login' do

    context 'when the user is not authorized for the database' do

      let(:cr) do
        described_class.new(user)
      end

      let(:login) do
        cr.login(connection).documents[0]
      end

      it 'logs the user into the connection' do
        expect {
          cr.login(connection)
        }.to raise_error(Mongo::Auth::Unauthorized)
      end
    end
  end
end

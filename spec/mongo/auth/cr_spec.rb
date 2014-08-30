require 'spec_helper'

describe Mongo::Auth::CR do

  let(:address) do
    Mongo::Server::Address.new('127.0.0.1:27017')
  end

  let(:connection) do
    Mongo::Connection.new(address, 5)
  end

  describe '#login' do

    context 'when the user is not authorized for the database' do

      let(:user) do
        Mongo::Auth::User.new(
          database: 'driver',
          user: 'test-user',
          password: 'password'
        )
      end

      let(:cr) do
        described_class.new(user)
      end

      let(:login) do
        cr.login(connection).documents[0]
      end

      it 'raises an exception' do
        expect {
          cr.login(connection)
        }.to raise_error(Mongo::Auth::Unauthorized)
      end
    end
  end

  pending 'when the user is authorized for the database' do

    let(:user) do
      Mongo::Auth::User.new(
        database: TEST_DB,
        user: ROOT_USER.name,
        password: ROOT_USER.password
      )
    end

    let(:cr) do
      described_class.new(user)
    end

    let(:login) do
      cr.login(connection).documents[0]
    end

    it 'logs the user into the connection' do
      expect(cr.login(connection).documents[0]['ok']).to eq(1)
    end
  end
end

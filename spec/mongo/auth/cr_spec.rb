require 'spec_helper'

describe Mongo::Auth::CR do

  let(:address) do
    Mongo::Server::Address.new('127.0.0.1:27017')
  end

  let(:connection) do
    Mongo::Connection.new(address, 5)
  end

  let(:user) do
    Mongo::Auth::User.new(TEST_DB, 'driver', 'password')
  end

  describe '#login' do

    context 'when the user is not authorized for the database' do

      let(:cr) do
        described_class.new(user)
      end

      let(:login) do
        cr.login(connection).documents[0]
      end

      # @todo Should probably raise an exception.
      it 'logs the user into the connection' do
        expect(login["errmsg"]).to_not be_nil
      end
    end
  end
end

require 'spec_helper'

describe Mongo::Auth::SCRAM do

  let(:address) do
    Mongo::Address.new(DEFAULT_ADDRESS)
  end

  let(:monitoring) do
    Mongo::Monitoring.new
  end

  let(:listeners) do
    Mongo::Event::Listeners.new
  end

  let(:server) do
    Mongo::Server.new(address, double('cluster'), monitoring, listeners, TEST_OPTIONS)
  end

  let(:connection) do
    Mongo::Server::Connection.new(server, TEST_OPTIONS)
  end

  describe '#login' do

    context 'when the user is not authorized' do

      let(:user) do
        Mongo::Auth::User.new(
          database: 'driver',
          user: 'notauser',
          password: 'password'
        )
      end

      let(:cr) do
        described_class.new(user)
      end

      it 'raises an exception', if: list_command_enabled? do
        expect {
          cr.login(connection)
        }.to raise_error(Mongo::Auth::Unauthorized)
      end
    end
  end

  context 'when the user is authorized for the database' do

    let(:cr) do
      described_class.new(root_user)
    end

    let(:login) do
      cr.login(connection).documents[0]
    end

    it 'logs the user into the connection', if: list_command_enabled? do
      expect(login['ok']).to eq(1)
    end
  end
end

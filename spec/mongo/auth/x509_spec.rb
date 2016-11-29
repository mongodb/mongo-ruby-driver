require 'spec_helper'

describe Mongo::Auth::X509 do

  let(:address) do
    default_address
  end

  let(:monitoring) do
    Mongo::Monitoring.new(monitoring: false)
  end

  let(:listeners) do
    Mongo::Event::Listeners.new
  end

  let(:cluster) do
    double('cluster').tap do |cl|
      allow(cl).to receive(:topology).and_return(topology)
      allow(cl).to receive(:app_metadata).and_return(app_metadata)
    end
  end

  let(:topology) do
    double('topology')
  end

  let(:server) do
    Mongo::Server.new(address, cluster, monitoring, listeners, TEST_OPTIONS)
  end

  let(:connection) do
    Mongo::Server::Connection.new(server, TEST_OPTIONS)
  end

  let(:user) do
    Mongo::Auth::User.new(database: TEST_DB, user: 'driver', password: 'password')
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

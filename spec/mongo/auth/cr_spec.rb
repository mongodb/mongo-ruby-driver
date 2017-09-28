require 'spec_helper'

describe Mongo::Auth::CR do

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
      allow(cl).to receive(:cluster_time).and_return(nil)
      allow(cl).to receive(:update_cluster_time)
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

      let(:login) do
        cr.login(connection).documents[0]
      end

      it 'raises an exception' do
        expect {
          cr.login(connection)
        }.to raise_error(Mongo::Auth::Unauthorized)
      end

      context 'when compression is used', if: testing_compression? do

        it 'does not compress the message' do
          expect(Mongo::Protocol::Compressed).not_to receive(:new)
          expect {
            cr.login(connection)
          }.to raise_error(Mongo::Auth::Unauthorized)
        end
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

    it 'logs the user into the connection', unless: scram_sha_1_enabled? do
      expect(cr.login(connection).documents[0]['ok']).to eq(1)
    end
  end
end

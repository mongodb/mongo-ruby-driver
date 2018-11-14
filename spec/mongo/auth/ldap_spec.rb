require 'spec_helper'

describe Mongo::Auth::LDAP do

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
      allow(cl).to receive(:options).and_return({})
      allow(cl).to receive(:cluster_time).and_return(nil)
      allow(cl).to receive(:update_cluster_time)
    end
  end

  declare_topology_double

  let(:server) do
    Mongo::Server.new(address, cluster, monitoring, listeners,
      SpecConfig.instance.test_options
    ).tap do |server|
      server.scan!
    end
  end

  let(:connection) do
    Mongo::Server::Connection.new(server, SpecConfig.instance.test_options)
  end

  let(:user) do
    Mongo::Auth::User.new(database: SpecConfig.instance.test_db, user: 'driver', password: 'password')
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

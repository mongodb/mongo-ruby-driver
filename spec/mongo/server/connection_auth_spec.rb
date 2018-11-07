require 'spec_helper'

# these tests fail intermittently in evergreen
describe Mongo::Server::Connection, retry: 3 do
  let(:address) do
    Mongo::Address.new(SpecConfig.instance.addresses.first)
  end

  let(:monitoring) do
    Mongo::Monitoring.new(monitoring: false)
  end

  let(:listeners) do
    Mongo::Event::Listeners.new
  end

  let(:app_metadata) do
    Mongo::Server::AppMetadata.new(SpecConfig.instance.test_options)
  end

  let(:cluster) do
    double('cluster').tap do |cl|
      allow(cl).to receive(:topology).and_return(topology)
      allow(cl).to receive(:app_metadata).and_return(app_metadata)
      allow(cl).to receive(:options).and_return({})
      allow(cl).to receive(:cluster_time).and_return(nil)
      allow(cl).to receive(:update_cluster_time)
      pool = double('pool')
      allow(pool).to receive(:disconnect!)
      allow(cl).to receive(:pool).and_return(pool)
    end
  end

  declare_topology_double

  let(:server) do
    Mongo::Server.new(address, cluster, monitoring, listeners,
      SpecConfig.instance.test_options.merge(monitoring_io: false))
  end

  before(:all) do
    ClientRegistry.instance.close_all_clients
  end

  describe '#auth_mechanism' do

    let(:connection) do
      described_class.new(server, server.options)
    end

    let(:reply) do
      double('reply').tap do |r|
        allow(r).to receive(:documents).and_return([ ismaster ])
      end
    end

    context 'when the ismaster response includes saslSupportedMechs' do
      min_server_version '4.0'

      let(:server_options) do
        SpecConfig.instance.test_options.merge(
          user: SpecConfig.instance.test_user.name,
          password: SpecConfig.instance.test_user.password,
          auth_source: SpecConfig.instance.test_db,
        )
      end

      let(:app_metadata) do
        Mongo::Server::AppMetadata.new(server_options)
      end

      before do
        client = authorized_client.with(database: SpecConfig.instance.test_db)
        info = client.database.users.info(SpecConfig.instance.test_user.name)
        expect(info.length).to eq(1)
        # this before block may have made 2 or 3 clients
        ClientRegistry.instance.close_all_clients
      end

      it 'uses scram256' do
        connection.connect!
        expect(connection.send(:default_mechanism)).to eq(:scram256)
      end
    end

    context 'when the ismaster response indicates the auth mechanism is :scram' do
      let(:features) do
        Mongo::Server::Description::Features.new(0..7)
      end

      it 'uses scram' do
        connection
        expect(Mongo::Server::Description::Features).to receive(:new).and_return(features)
        connection.connect!
        expect(connection.send(:default_mechanism)).to eq(:scram)
      end
    end

    context 'when the ismaster response indicates the auth mechanism is :mongodb_cr' do
      let(:features) do
        Mongo::Server::Description::Features.new(0..2)
      end

      it 'uses mongodb_cr' do
        connection
        expect(Mongo::Server::Description::Features).to receive(:new).and_return(features)
        connection.connect!
        expect(connection.send(:default_mechanism)).to eq(:mongodb_cr)
      end
    end
  end
end

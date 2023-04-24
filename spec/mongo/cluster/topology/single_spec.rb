# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe Mongo::Cluster::Topology::Single do

  let(:address) do
    Mongo::Address.new('127.0.0.1:27017')
  end

  let(:monitoring) do
    Mongo::Monitoring.new(monitoring: false)
  end

  # Cluster needs a topology and topology needs a cluster...
  # This temporary cluster is used for topology construction.
  let(:temp_cluster) do
    double('temp cluster').tap do |cluster|
      allow(cluster).to receive(:servers_list).and_return([])
    end
  end

  let(:topology) do
    described_class.new({}, monitoring, temp_cluster)
  end

  let(:listeners) do
    Mongo::Event::Listeners.new
  end

  let(:cluster) do
    double('cluster').tap do |cl|
      allow(cl).to receive(:app_metadata).and_return(app_metadata)
      allow(cl).to receive(:topology).and_return(topology)
      allow(cl).to receive(:options).and_return({})
    end
  end

  describe '.servers' do

    let(:mongos) do
      Mongo::Server.new(address, cluster, monitoring, listeners,
        SpecConfig.instance.test_options.merge(monitoring_io: false)
      ).tap do |server|
        allow(server).to receive(:description).and_return(mongos_description)
      end
    end

    let(:standalone) do
      Mongo::Server.new(address, cluster, monitoring, listeners,
        SpecConfig.instance.test_options.merge(monitoring_io: false)
      ).tap do |server|
        allow(server).to receive(:description).and_return(standalone_description)
      end
    end

    let(:standalone_two) do
      Mongo::Server.new(address, cluster, monitoring, listeners,
        SpecConfig.instance.test_options.merge(monitoring_io: false)
      ).tap do |server|
        allow(server).to receive(:description).and_return(standalone_description)
      end
    end

    let(:replica_set) do
      Mongo::Server.new(address, cluster, monitoring, listeners,
        SpecConfig.instance.test_options.merge(monitoring_io: false)
      ).tap do |server|
        allow(server).to receive(:description).and_return(replica_set_description)
      end
    end

    let(:mongos_description) do
      Mongo::Server::Description.new(address, { 'msg' => 'isdbgrid' })
    end

    let(:standalone_description) do
      Mongo::Server::Description.new(address, { 'isWritablePrimary' => true,
        'minWireVersion' => 2, 'maxWireVersion' => 8, 'ok' => 1 })
    end

    let(:replica_set_description) do
      Mongo::Server::Description.new(address, { 'isWritablePrimary' => true,
        'minWireVersion' => 2, 'maxWireVersion' => 8,
        'setName' => 'testing' })
    end

    let(:servers) do
      topology.servers([ mongos, standalone, standalone_two, replica_set ])
    end

    it 'returns all data-bearing non-unknown servers' do
      # mongos and replica_set do not have ok: 1 in their descriptions,
      # and are considered unknown.
      expect(servers).to eq([ standalone, standalone_two ])
    end
  end

  describe '#initialize' do
    context 'with RS name' do
      let(:topology) do
        Mongo::Cluster::Topology::Single.new(
          {replica_set_name: 'foo'},
          monitoring, temp_cluster)
      end

      it 'accepts RS name' do
        expect(topology.replica_set_name).to eq('foo')
      end
    end

    context 'with more than one server in topology' do
      let(:topology) do
        Mongo::Cluster::Topology::Single.new({},
          monitoring, temp_cluster)
      end

      let(:server_1) do
        double('server').tap do |server|
          allow(server).to receive(:address).and_return(Mongo::Address.new('one'))
        end
      end

      let(:server_2) do
        double('server').tap do |server|
          allow(server).to receive(:address).and_return(Mongo::Address.new('two'))
        end
      end

      let(:temp_cluster) do
        double('temp cluster').tap do |cluster|
          allow(cluster).to receive(:servers_list).and_return([server_1, server_2])
        end
      end

      it 'fails' do
        expect do
          topology
        end.to raise_error(ArgumentError, /Cannot instantiate a single topology with more than one server in the cluster: one, two/)
      end
    end
  end

  describe '.replica_set?' do

    it 'returns false' do
      expect(topology).to_not be_replica_set
    end
  end

  describe '.sharded?' do

    it 'returns false' do
      expect(topology).to_not be_sharded
    end
  end

  describe '.single?' do

    it 'returns true' do
      expect(topology).to be_single
    end
  end

  describe '#has_readable_servers?' do

    it 'returns true' do
      expect(topology).to have_readable_server(nil, nil)
    end
  end

  describe '#has_writable_servers?' do

    it 'returns true' do
      expect(topology).to have_writable_server(nil)
    end
  end

  describe '#summary' do
    require_no_linting

    let(:desc) do
      Mongo::Server::Description.new(Mongo::Address.new('127.0.0.2:27017'))
    end

    it 'renders correctly' do
      expect(topology).to receive(:server_descriptions).and_return({desc.address.to_s => desc})
      expect(topology.summary).to eq('Single[127.0.0.2:27017]')
    end
  end
end

# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe Mongo::Cluster::Topology::ReplicaSetNoPrimary do

  let(:address) do
    Mongo::Address.new('127.0.0.1:27017')
  end

  let(:listeners) do
    Mongo::Event::Listeners.new
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

  let(:cluster) do
    double('cluster').tap do |cl|
      allow(cl).to receive(:topology).and_return(topology)
      allow(cl).to receive(:app_metadata).and_return(app_metadata)
      allow(cl).to receive(:options).and_return({})
    end
  end

  describe '#servers' do

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

    let(:replica_set) do
      Mongo::Server.new(address, cluster, monitoring, listeners,
        SpecConfig.instance.test_options.merge(monitoring_io: false)
      ).tap do |server|
        allow(server).to receive(:description).and_return(replica_set_description)
      end
    end

    let(:replica_set_two) do
      Mongo::Server.new(address, cluster, monitoring, listeners,
        SpecConfig.instance.test_options.merge(monitoring_io: false)
      ).tap do |server|
        allow(server).to receive(:description).and_return(replica_set_two_description)
      end
    end

    let(:mongos_description) do
      Mongo::Server::Description.new(address, { 'msg' => 'isdbgrid',
        'minWireVersion' => 2, 'maxWireVersion' => 8, 'ok' => 1 })
    end

    let(:standalone_description) do
      Mongo::Server::Description.new(address, { 'isWritablePrimary' => true,
        'minWireVersion' => 2, 'maxWireVersion' => 8, 'ok' => 1 })
    end

    let(:replica_set_description) do
      Mongo::Server::Description.new(address, { 'isWritablePrimary' => true,
        'minWireVersion' => 2, 'maxWireVersion' => 8,
        'setName' => 'testing', 'ok' => 1 })
    end

    let(:replica_set_two_description) do
      Mongo::Server::Description.new(address, { 'isWritablePrimary' => true,
        'minWireVersion' => 2, 'maxWireVersion' => 8,
        'setName' => 'test', 'ok' => 1 })
    end

    context 'when a replica set name is provided' do

      let(:topology) do
        described_class.new({ :replica_set_name => 'testing' }, monitoring, temp_cluster)
      end

      let(:servers) do
        topology.servers([ mongos, standalone, replica_set, replica_set_two ])
      end

      it 'returns only replica set members is the provided set' do
        expect(servers).to eq([ replica_set ])
      end
    end
  end

  describe '.replica_set?' do

    it 'returns true' do
      expect(described_class.new({replica_set_name: 'foo'}, monitoring, temp_cluster)).to be_replica_set
    end
  end

  describe '.sharded?' do

    it 'returns false' do
      expect(described_class.new({replica_set_name: 'foo'}, monitoring, temp_cluster)).to_not be_sharded
    end
  end

  describe '.single?' do

    it 'returns false' do
      expect(described_class.new({replica_set_name: 'foo'}, monitoring, temp_cluster)).to_not be_single
    end
  end

  describe '#max_election_id' do
    let(:election_id) { BSON::ObjectId.new }

    it 'returns value given in constructor options' do
      topology = described_class.new({replica_set_name: 'foo', max_election_id: election_id},
        monitoring, temp_cluster)

      expect(topology.max_election_id).to eql(election_id)
    end
  end

  describe '#max_set_version' do
    it 'returns value given in constructor options' do
      topology = described_class.new({replica_set_name: 'foo', max_set_version: 5},
        monitoring, temp_cluster)

      expect(topology.max_set_version).to eq(5)
    end
  end

  describe '#has_readable_servers?' do

    let(:topology) do
      described_class.new({replica_set_name: 'foo'}, monitoring, temp_cluster)
    end

    let(:cluster) do
      double('cluster',
        servers: servers,
        single?: false,
        replica_set?: true,
        sharded?: false,
        unknown?: false,
      )
    end

    context 'when the read preference is primary' do

      let(:selector) do
        Mongo::ServerSelector.get(:mode => :primary)
      end

      context 'when a primary exists' do

        let(:servers) do
          [ double('server',
            primary?: true,
            # for runs with linting enabled
            average_round_trip_time: 42,
          ) ]
        end

        it 'returns true' do
          expect(topology).to have_readable_server(cluster, selector)
        end
      end

      context 'when a primary does not exist' do

        let(:servers) do
          [ double('server', primary?: false) ]
        end

        it 'returns false' do
          expect(topology).to_not have_readable_server(cluster, selector)
        end
      end
    end

    context 'when the read preference is primary preferred' do

      let(:selector) do
        Mongo::ServerSelector.get(:mode => :primary_preferred)
      end

      context 'when a primary exists' do

        let(:servers) do
          [ double('server',
            primary?: true,
            secondary?: false,
            # for runs with linting enabled
            average_round_trip_time: 42,
          ) ]
        end

        it 'returns true' do
          expect(topology).to have_readable_server(cluster, selector)
        end
      end

      context 'when a primary does not exist' do

        let(:servers) do
          [ double('server', primary?: false, secondary?: true, average_round_trip_time: 0.01) ]
        end

        it 'returns true' do
          expect(topology).to have_readable_server(cluster, selector)
        end
      end
    end

    context 'when the read preference is secondary' do

      let(:selector) do
        Mongo::ServerSelector.get(:mode => :secondary)
      end

      context 'when a secondary exists' do

        let(:servers) do
          [ double('server', primary?: false, secondary?: true, average_round_trip_time: 0.01) ]
        end

        it 'returns true' do
          expect(topology).to have_readable_server(cluster, selector)
        end
      end

      context 'when a secondary does not exist' do

        let(:servers) do
          [ double('server', primary?: true, secondary?: false) ]
        end

        it 'returns false' do
          expect(topology).to_not have_readable_server(cluster, selector)
        end
      end
    end

    context 'when the read preference is secondary preferred' do

      let(:selector) do
        Mongo::ServerSelector.get(:mode => :secondary_preferred)
      end

      context 'when a secondary exists' do

        let(:servers) do
          [ double('server', primary?: false, secondary?: true, average_round_trip_time: 0.01) ]
        end

        it 'returns true' do
          expect(topology).to have_readable_server(cluster, selector)
        end
      end

      context 'when a secondary does not exist' do

        let(:servers) do
          [ double('server',
            secondary?: false,
            primary?: true,
            # for runs with linting enabled
            average_round_trip_time: 42,
          ) ]
        end

        it 'returns true' do
          expect(topology).to have_readable_server(cluster, selector)
        end
      end
    end

    context 'when the read preference is nearest' do

      let(:selector) do
        Mongo::ServerSelector.get(:mode => :nearest)
      end

      let(:servers) do
        [ double('server', primary?: false, secondary?: true, average_round_trip_time: 0.01) ]
      end

      it 'returns true' do
        expect(topology).to have_readable_server(cluster, selector)
      end
    end

    context 'when the read preference is not provided' do

      context 'when a primary exists' do

        let(:servers) do
          [ double('server',
            primary?: true,
            secondary?: false,
            # for runs with linting enabled
            average_round_trip_time: 42,
          ) ]
        end

        it 'returns true' do
          expect(topology).to have_readable_server(cluster)
        end
      end

      context 'when a primary does not exist' do

        let(:servers) do
          [ double('server', primary?: false, secondary?: true, average_round_trip_time: 0.01) ]
        end

        it 'returns false' do
          expect(topology).to_not have_readable_server(cluster)
        end
      end
    end
  end

  describe '#has_writable_servers?' do

    let(:topology) do
      described_class.new({replica_set_name: 'foo'}, monitoring, temp_cluster)
    end

    context 'when a primary server exists' do

      let(:primary) do
        double('server',
          :primary? => true,
          # for runs with linting enabled
          average_round_trip_time: 42,
        )
      end

      let(:secondary) do
        double('server',
          :primary? => false,
          # for runs with linting enabled
          average_round_trip_time: 42,
        )
      end

      let(:cluster) do
        double('cluster',
          single?: false,
          replica_set?: true,
          sharded?: false,
          servers: [ primary, secondary ],
        )
      end

      it 'returns true' do
        expect(topology).to have_writable_server(cluster)
      end
    end

    context 'when no primary server exists' do

      let(:server) do
        double('server', :primary? => false)
      end

      let(:cluster) do
        double('cluster',
          single?: false,
          replica_set?: true,
          sharded?: false,
          servers: [ server ],
        )
      end

      it 'returns false' do
        expect(topology).to_not have_writable_server(cluster)
      end
    end
  end

  describe '#new_max_set_version' do
    context 'initially nil' do
      let(:topology) do
        described_class.new({replica_set_name: 'foo'}, monitoring, temp_cluster).tap do |topology|
          expect(topology.max_set_version).to be nil
        end
      end

      context 'description with non-nil max set version' do
        let(:description) do
          Mongo::Server::Description.new('a', { 'setVersion' => 5 }).tap do |description|
            expect(description.set_version).to eq(5)
          end
        end

        it 'is set to max set version in description' do
          expect(topology.new_max_set_version(description)).to eq(5)
        end
      end

      context 'description with nil max set version' do
        let(:description) do
          Mongo::Server::Description.new('a').tap do |description|
            expect(description.set_version).to be nil
          end
        end

        it 'is nil' do
          expect(topology.new_max_set_version(description)).to be nil
        end
      end
    end

    context 'initially not nil' do
      let(:topology) do
        described_class.new({replica_set_name: 'foo', max_set_version: 4},
          monitoring, temp_cluster
        ).tap do |topology|
          expect(topology.max_set_version).to eq(4)
        end
      end

      context 'description with a higher max set version' do
        let(:description) do
          Mongo::Server::Description.new('a', { 'setVersion' => 5 }).tap do |description|
            expect(description.set_version).to eq(5)
          end
        end

        it 'is set to max set version in description' do
          expect(topology.new_max_set_version(description)).to eq(5)
        end
      end

      context 'description with a lower max set version' do
        let(:description) do
          Mongo::Server::Description.new('a', { 'setVersion' => 3 }).tap do |description|
            expect(description.set_version).to eq(3)
          end
        end

        it 'is set to topology max set version' do
          expect(topology.new_max_set_version(description)).to eq(4)
        end
      end

      context 'description with nil max set version' do
        let(:description) do
          Mongo::Server::Description.new('a').tap do |description|
            expect(description.set_version).to be nil
          end
        end

        it 'is set to topology max set version' do
          expect(topology.new_max_set_version(description)).to eq(4)
        end
      end
    end
  end

  describe '#new_max_election_id' do
    context 'initially nil' do
      let(:topology) do
        described_class.new({replica_set_name: 'foo'},
          monitoring, temp_cluster,
        ).tap do |topology|
          expect(topology.max_election_id).to be nil
        end
      end

      context 'description with non-nil max election id' do
        let(:new_election_id) { BSON::ObjectId.from_string('7fffffff000000000000004f') }

        let(:description) do
          Mongo::Server::Description.new('a', { 'electionId' => new_election_id }).tap do |description|
            expect(description.election_id).to be new_election_id
          end
        end

        it 'is set to max election id in description' do
          expect(topology.new_max_election_id(description)).to be new_election_id
        end
      end

      context 'description with nil max election id' do
        let(:description) do
          Mongo::Server::Description.new('a').tap do |description|
            expect(description.election_id).to be nil
          end
        end

        it 'is nil' do
          expect(topology.new_max_election_id(description)).to be nil
        end
      end
    end

    context 'initially not nil' do
      let(:old_election_id) { BSON::ObjectId.from_string('7fffffff000000000000004c') }

      let(:topology) do
        described_class.new({replica_set_name: 'foo', max_election_id: old_election_id},
          monitoring, temp_cluster,
        ).tap do |topology|
          expect(topology.max_election_id).to be old_election_id
        end
      end

      context 'description with a higher max election id' do
        let(:new_election_id) { BSON::ObjectId.from_string('7fffffff000000000000004f') }

        let(:description) do
          Mongo::Server::Description.new('a', { 'electionId' => new_election_id }).tap do |description|
            expect(description.election_id).to be new_election_id
          end
        end

        it 'is set to max election id in description' do
          expect(topology.new_max_election_id(description)).to be new_election_id
        end
      end

      context 'description with a lower max election id' do
        let(:low_election_id) { BSON::ObjectId.from_string('7fffffff0000000000000042') }

        let(:description) do
          Mongo::Server::Description.new('a', { 'electionId' => low_election_id }).tap do |description|
            expect(description.election_id).to be low_election_id
          end
        end

        it 'is set to topology max election id' do
          expect(topology.new_max_election_id(description)).to be old_election_id
        end
      end

      context 'description with nil max election id' do
        let(:description) do
          Mongo::Server::Description.new('a').tap do |description|
            expect(description.election_id).to be nil
          end
        end

        it 'is set to topology max election id' do
          expect(topology.new_max_election_id(description)).to be old_election_id
        end
      end
    end
  end

  describe '#summary' do
    require_no_linting

    let(:desc) do
      Mongo::Server::Description.new(Mongo::Address.new('127.0.0.2:27017'))
    end

    let(:topology) do
      described_class.new({replica_set_name: 'foo'}, monitoring, temp_cluster)
    end

    it 'renders correctly' do
      expect(topology).to receive(:server_descriptions).and_return({desc.address.to_s => desc})
      expect(topology.summary).to eq('ReplicaSetNoPrimary[127.0.0.2:27017,name=foo]')
    end

    context 'with max set version and max election id' do
      let(:topology) do
        described_class.new({
          replica_set_name: 'foo',
          max_set_version: 5,
          max_election_id: BSON::ObjectId.from_string('7fffffff0000000000000042'),
        }, monitoring, temp_cluster)
      end

      it 'renders correctly' do
        expect(topology).to receive(:server_descriptions).and_return({desc.address.to_s => desc})
        expect(topology.summary).to eq('ReplicaSetNoPrimary[127.0.0.2:27017,name=foo,v=5,e=7fffffff0000000000000042]')
      end
    end
  end
end

require 'spec_helper'

describe Mongo::Server::Description do

  let(:replica) do
    {
      'setName' => 'mongodb_set',
      'ismaster' => true,
      'secondary' => false,
      'hosts' => [
        '127.0.0.1:27018',
        '127.0.0.1:27019'
      ],
      'arbiters' => [
        '127.0.0.1:27120'
      ],
      'primary' => '127.0.0.1:27019',
      'tags' => { 'rack' => 'a' },
      'me' => '127.0.0.1:27019',
      'maxBsonObjectSize' => 16777216,
      'maxMessageSizeBytes' => 48000000,
      'maxWriteBatchSize' => 1000,
      'maxWireVersion' => 2,
      'minWireVersion' => 0,
      'localTime' => Time.now,
      'lastWrite' => { 'lastWriteDate' => Time.now },
      'ok' => 1
    }
  end

  let(:address) do
    Mongo::Address.new('127.0.0.1:27017')
  end

  let(:monitoring) do
    Mongo::Monitoring.new(monitoring: false)
  end

  let(:topology) do
    double('topology')
  end
  
  let(:cluster) do
    double('cluster').tap do |cl|
      allow(cl).to receive(:topology).and_return(topology)
      allow(cl).to receive(:app_metadata).and_return(app_metadata)
    end
  end

  describe '#arbiter?' do

    context 'when the server is an arbiter' do

      let(:description) do
        described_class.new(address, { 'arbiterOnly' => true, 'setName' => 'test' })
      end

      it 'returns true' do
        expect(description).to be_arbiter
      end
    end

    context 'when the server is not an arbiter' do

      let(:description) do
        described_class.new(address, replica)
      end

      it 'returns false' do
        expect(description).to_not be_arbiter
      end
    end
  end

  describe '#arbiters' do

    context 'when the replica set has arbiters' do

      let(:description) do
        described_class.new(address, replica)
      end

      it 'returns the arbiters' do
        expect(description.arbiters).to eq([ '127.0.0.1:27120' ])
      end
    end

    context 'when the replica set has no arbiters' do

      let(:description) do
        described_class.new(address, {})
      end

      it 'returns an empty array' do
        expect(description.arbiters).to be_empty
      end
    end

    context 'when the addresses are not lowercase' do

      let(:config) do
        replica.merge(
                   {
                       'arbiters' => [
                           'SERVER:27017'
                       ],
                   }
        )
      end

      let(:description) do
        described_class.new(address, config)
      end

      it 'normalizes the addresses to lowercase' do
        expect(description.arbiters).to eq(['server:27017'])
      end
    end
  end

  describe '#ghost?' do

    context 'when the server is a ghost' do

      let(:config) do
        { 'isreplicaset' => true }
      end

      let(:description) do
        described_class.new(address, config)
      end

      it 'returns true' do
        expect(description).to be_ghost
      end
    end

    context 'when the server is not a ghost' do

      let(:description) do
        described_class.new(address, replica)
      end

      it 'returns false' do
        expect(description).to_not be_ghost
      end
    end
  end

  describe '#hidden?' do

    context 'when the server is hidden' do

      let(:description) do
        described_class.new(address, { 'hidden' => true })
      end

      it 'returns true' do
        expect(description).to be_hidden
      end
    end

    context 'when the server is not hidden' do

      let(:description) do
        described_class.new(address, replica)
      end

      it 'returns false' do
        expect(description).to_not be_hidden
      end
    end
  end

  describe '#hosts' do

    let(:description) do
      described_class.new(address, replica)
    end

    it 'returns all the hosts in the replica set' do
      expect(description.hosts).to eq([ '127.0.0.1:27018', '127.0.0.1:27019' ])
    end

    context 'when the addresses are not lowercase' do

      let(:config) do
        replica.merge(
            {
                'hosts' => [
                    'SERVER:27017'
                ],
            }
        )
      end

      let(:description) do
        described_class.new(address, config)
      end

      it 'normalizes the addresses to lowercase' do
        expect(description.hosts).to eq(['server:27017'])
      end
    end
  end

  describe '#max_bson_object_size' do

    let(:description) do
      described_class.new(address, replica)
    end

    it 'returns the value' do
      expect(description.max_bson_object_size).to eq(16777216)
    end
  end

  describe '#max_message_size' do

    let(:description) do
      described_class.new(address, replica)
    end

    it 'returns the value' do
      expect(description.max_message_size).to eq(48000000)
    end
  end

  describe '#max_write_batch_size' do

    let(:description) do
      described_class.new(address, replica)
    end

    it 'returns the value' do
      expect(description.max_write_batch_size).to eq(1000)
    end
  end

  describe '#max_wire_version' do

    context 'when the max wire version is provided' do

      let(:description) do
        described_class.new(address, replica)
      end

      it 'returns the value' do
        expect(description.max_wire_version).to eq(2)
      end
    end

    context 'when the max wire version is not provided' do

      let(:description) do
        described_class.new(address, {})
      end

      it 'returns the default' do
        expect(description.max_wire_version).to eq(0)
      end
    end
  end

  describe '#min_wire_version' do

    context 'when the min wire version is provided' do

      let(:description) do
        described_class.new(address, replica)
      end

      it 'returns the value' do
        expect(description.min_wire_version).to eq(0)
      end
    end

    context 'when the min wire version is not provided' do

      let(:description) do
        described_class.new(address, {})
      end

      it 'returns the default' do
        expect(description.min_wire_version).to eq(0)
      end
    end
  end

  describe '#tags' do

    context 'when the server has tags' do

      let(:description) do
        described_class.new(address, replica)
      end

      it 'returns the tags' do
        expect(description.tags).to eq(replica['tags'])
      end
    end

    context 'when the server does not have tags' do

      let(:config) do
        { 'ismaster' => true }
      end

      let(:description) do
        described_class.new(address, config)
      end

      it 'returns an empty hash' do
        expect(description.tags).to eq({})
      end
    end
  end

  describe '#mongos?' do

    context 'when the server is a mongos' do

      let(:config) do
        { 'msg' => 'isdbgrid', 'ismaster' => true }
      end

      let(:description) do
        described_class.new(address, config)
      end

      it 'returns true' do
        expect(description).to be_mongos
      end
    end

    context 'when the server is not a mongos' do

      let(:description) do
        described_class.new(address, replica)
      end

      it 'returns false' do
        expect(description).to_not be_mongos
      end
    end
  end

  describe '#passive?' do

    context 'when the server is passive' do

      let(:description) do
        described_class.new(address, { 'passive' => true })
      end

      it 'returns true' do
        expect(description).to be_passive
      end
    end

    context 'when the server is not passive' do

      let(:description) do
        described_class.new(address, replica)
      end

      it 'returns false' do
        expect(description).to_not be_passive
      end
    end
  end

  describe '#passives' do

    context 'when passive servers exists' do

      let(:description) do
        described_class.new(address, { 'passives' => [ '127.0.0.1:27025' ] })
      end

      it 'returns a list of the passives' do
        expect(description.passives).to eq([ '127.0.0.1:27025' ])
      end
    end

    context 'when no passive servers exist' do

      let(:description) do
        described_class.new(address, replica)
      end

      it 'returns an empty array' do
        expect(description.passives).to be_empty
      end
    end

    context 'when the addresses are not lowercase' do

      let(:config) do
        replica.merge(
            {
                'passives' => [
                    'SERVER:27017'
                ],
            }
        )
      end

      let(:description) do
        described_class.new(address, config)
      end

      it 'normalizes the addresses to lowercase' do
        expect(description.passives).to eq(['server:27017'])
      end

      it 'normalizes the addresses to lowercase' do

      end
    end
  end

  describe '#primary?' do

    context 'when the server is not a primary' do

      let(:description) do
        described_class.new(address, { 'ismaster' => false })
      end

      it 'returns true' do
        expect(description).to_not be_primary
      end
    end

    context 'when the server is a primary' do

      let(:description) do
        described_class.new(address, replica)
      end

      it 'returns false' do
        expect(description).to be_primary
      end
    end
  end

  describe '#average_round_trip_time' do

    let(:description) do
      described_class.new(address, { 'secondary' => false }, 4.5)
    end

    it 'defaults to 0' do
      expect(described_class.new(address).average_round_trip_time).to eq(0)
    end

    it 'can be set via the constructor' do
      expect(description.average_round_trip_time).to eq(4.5)
    end
  end

  describe '#replica_set_name' do

    context 'when the server is in a replica set' do

      let(:description) do
        described_class.new(address, replica)
      end

      it 'returns the replica set name' do
        expect(description.replica_set_name).to eq('mongodb_set')
      end
    end

    context 'when the server is not in a replica set' do

      let(:description) do
        described_class.new(address, {})
      end

      it 'returns nil' do
        expect(description.replica_set_name).to be_nil
      end
    end
  end

  describe '#secondary?' do

    context 'when the server is not a secondary' do

      let(:description) do
        described_class.new(address, { 'secondary' => false })
      end

      it 'returns true' do
        expect(description).to_not be_secondary
      end
    end

    context 'when the server is a secondary' do

      let(:description) do
        described_class.new(address, { 'secondary' => true, 'setName' => 'test' })
      end

      it 'returns false' do
        expect(description).to be_secondary
      end
    end
  end

  describe '#servers' do

    let(:config) do
      replica.merge({ 'passives' => [ '127.0.0.1:27025' ]})
    end

    let(:description) do
      described_class.new(address, config)
    end

    it 'returns the hosts + arbiters + passives' do
      expect(description.servers).to eq(
        [ '127.0.0.1:27018', '127.0.0.1:27019', '127.0.0.1:27120', '127.0.0.1:27025' ]
      )
    end
  end

  describe '#standalone?' do

    context 'when the server is standalone' do

      let(:description) do
        described_class.new(address, { 'ismaster' => true, 'ok' => 1 })
      end

      it 'returns true' do
        expect(description).to be_standalone
      end
    end

    context 'when the server is part of a replica set' do

      let(:description) do
        described_class.new(address, replica)
      end

      it 'returns false' do
        expect(description).to_not be_standalone
      end
    end
  end

  describe '#server_type' do

    context 'when the server is an arbiter' do

      let(:description) do
        described_class.new(address, { 'arbiterOnly' => true, 'setName' => 'test' })
      end

      it 'returns :arbiter' do
        expect(description.server_type).to eq(:arbiter)
      end
    end

    context 'when the server is a ghost' do

      let(:description) do
        described_class.new(address, { 'isreplicaset' => true })
      end

      it 'returns :ghost' do
        expect(description.server_type).to eq(:ghost)
      end
    end

    context 'when the server is a mongos' do

      let(:config) do
        { 'msg' => 'isdbgrid', 'ismaster' => true }
      end

      let(:description) do
        described_class.new(address, config)
      end

      it 'returns :sharded' do
        expect(description.server_type).to eq(:sharded)
      end
    end

    context 'when the server is a primary' do

      let(:description) do
        described_class.new(address, replica)
      end

      it 'returns :primary' do
        expect(description.server_type).to eq(:primary)
      end
    end

    context 'when the server is a secondary' do

      let(:description) do
        described_class.new(address, { 'secondary' => true, 'setName' => 'test' })
      end

      it 'returns :secondary' do
        expect(description.server_type).to eq(:secondary)
      end
    end

    context 'when the server is standalone' do

      let(:description) do
        described_class.new(address, { 'ismaster' => true, 'ok' => 1 })
      end

      it 'returns :standalone' do
        expect(description.server_type).to eq(:standalone)
      end
    end

    context 'when the description has no configuration' do

      let(:description) do
        described_class.new(address)
      end

      it 'returns :unknown' do
        expect(description.server_type).to eq(:unknown)
      end
    end
  end

  describe '#unknown?' do

    context 'when the description has no configuration' do

      let(:description) do
        described_class.new(address)
      end

      it 'returns true' do
        expect(description).to be_unknown
      end
    end

    context 'when the command was not ok' do

      let(:description) do
        described_class.new(address, { 'ok' => 0 })
      end

      it 'returns true' do
        expect(description).to be_unknown
      end
    end

    context 'when the description has a configuration' do

      let(:config) do
        { 'hosts' => [ '127.0.0.1:27019', '127.0.0.1:27020' ], 'ok' => 1 }
      end

      let(:description) do
        described_class.new(address, config)
      end

      it 'returns false' do
        expect(description).to_not be_unknown
      end
    end
  end

  describe '#is_server?' do

    let(:listeners) do
      Mongo::Event::Listeners.new
    end

    let(:server) do
      Mongo::Server.new(address, cluster, monitoring, listeners)
    end

    let(:description) do
      described_class.new(address, {})
    end

    context 'when the server address matches the description address' do

      it 'returns true' do
        expect(description.is_server?(server)).to be(true)
      end
    end

    context 'when the server address does not match the description address' do

      let(:other_address) do
        Mongo::Address.new('127.0.0.1:27020')
      end

      let(:server) do
        Mongo::Server.new(other_address, cluster, monitoring, listeners)
      end

      it 'returns false' do
        expect(description.is_server?(server)).to be(false)
      end
    end
  end

  describe '#me_mismatch?' do

    let(:description) do
      described_class.new(address, config)
    end

    context 'when the server address matches the me field' do

      let(:config) do
        replica.merge('me' => address.to_s)
      end

      it 'returns false' do
        expect(description.me_mismatch?).to be(false)
      end
    end

    context 'when the server address does not match the me field' do

      let(:config) do
        replica.merge('me' => 'localhost:27020')
      end

      it 'returns true' do
        expect(description.me_mismatch?).to be(true)
      end
    end

    context 'when there is no me field' do

      let(:config) do
        replica.tap do |r|
          r.delete('me')
        end
      end

      it 'returns false' do
        expect(description.me_mismatch?).to be(false)
      end
    end
  end

  describe '#lists_server?' do

    let(:description) do
      described_class.new(address, replica)
    end

    let(:server_address) do
      Mongo::Address.new('127.0.0.1:27018')
    end

    let(:listeners) do
      Mongo::Event::Listeners.new
    end

    let(:server) do
      Mongo::Server.new(server_address, cluster, monitoring, listeners)
    end

    context 'when the server is included in the description hosts list' do

      it 'returns true' do
        expect(description.lists_server?(server)).to be(true)
      end
    end

    context 'when the server is not included in the description hosts list' do

      let(:server_address) do
        Mongo::Address.new('127.0.0.1:27017')
      end

      it 'returns false' do
        expect(description.lists_server?(server)).to be(false)
      end
    end
  end

  describe '#replica_set_member?' do

    context 'when the description is from a mongos' do

      let(:config) do
        { 'msg' => 'isdbgrid', 'ismaster' => true }
      end

      let(:description) do
        described_class.new(address, config)
      end

      it 'returns false' do
        expect(description.replica_set_member?).to be(false)
      end
    end

    context 'when the description is from a standalone' do

      let(:description) do
        described_class.new(address, { 'ismaster' => true, 'ok' => 1 })
      end

      it 'returns false' do
        expect(description.replica_set_member?).to be(false)
      end
    end

    context 'when the description is from a replica set member' do

      let(:description) do
        described_class.new(address, replica)
      end

      it 'returns true' do
        expect(description.replica_set_member?).to be(true)
      end
    end
  end

  describe '#==' do

    let(:description) do
      described_class.new(address, replica)
    end

    let(:other) do
      described_class.new(address, replica.merge(
        'localTime' => 1,
        'lastWrite' => { 'lastWriteDate' => 1 }
      ))
    end

    it 'excludes certain fields' do
      expect(description == other).to be(true)
    end

    context 'when the classes do not match' do

      let(:description) do
        described_class.new(address, replica)
      end

      it 'returns false' do
        expect(description == Array.new).to be(false)
      end
    end

    context 'when the configs match' do

      let(:description) do
        described_class.new(address, replica)
      end

      let(:other) do
        described_class.new(address, replica)
      end

      it 'returns true' do
        expect(description == other).to be(true)
      end
    end

    context 'when the configs do not match' do

      let(:description) do
        described_class.new(address, replica)
      end

      let(:other) do
        described_class.new(address, { 'ismaster' => true, 'ok' => 1 })
      end

      it 'returns false' do
        expect(description == other).to be(false)
      end
    end
  end
end

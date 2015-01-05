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
      'me' => '127.0.0.1:27019',
      'maxBsonObjectSize' => 16777216,
      'maxMessageSizeBytes' => 48000000,
      'maxWriteBatchSize' => 1000,
      'maxWireVersion' => 2,
      'minWireVersion' => 0,
      'ok' => 1
    }
  end

  describe '#arbiter?' do

    context 'when the server is an arbiter' do

      let(:description) do
        described_class.new({ 'arbiterOnly' => true, 'setName' => 'test' })
      end

      it 'returns true' do
        expect(description).to be_arbiter
      end
    end

    context 'when the server is not an arbiter' do

      let(:description) do
        described_class.new(replica)
      end

      it 'returns false' do
        expect(description).to_not be_arbiter
      end
    end
  end

  describe '#arbiters' do

    context 'when the replica set has arbiters' do

      let(:description) do
        described_class.new(replica)
      end

      it 'returns the arbiters' do
        expect(description.arbiters).to eq([ '127.0.0.1:27120' ])
      end
    end

    context 'when the replica set has no arbiters' do

      let(:description) do
        described_class.new({})
      end

      it 'returns an empty array' do
        expect(description.arbiters).to be_empty
      end
    end
  end

  describe '#ghost?' do

    context 'when the server is a ghost' do

      let(:config) do
        { 'isreplicaset' => true }
      end

      let(:description) do
        described_class.new(config)
      end

      it 'returns true' do
        expect(description).to be_ghost
      end
    end

    context 'when the server is not a ghost' do

      let(:description) do
        described_class.new(replica)
      end

      it 'returns false' do
        expect(description).to_not be_ghost
      end
    end
  end

  describe '#hidden?' do

    context 'when the server is hidden' do

      let(:description) do
        described_class.new({ 'hidden' => true })
      end

      it 'returns true' do
        expect(description).to be_hidden
      end
    end

    context 'when the server is not hidden' do

      let(:description) do
        described_class.new(replica)
      end

      it 'returns false' do
        expect(description).to_not be_hidden
      end
    end
  end

  describe '#hosts' do

    let(:description) do
      described_class.new(replica)
    end

    it 'returns all the hosts in the replica set' do
      expect(description.hosts).to eq([ '127.0.0.1:27018', '127.0.0.1:27019' ])
    end
  end

  describe '#max_bson_object_size' do

    let(:description) do
      described_class.new(replica)
    end

    it 'returns the value' do
      expect(description.max_bson_object_size).to eq(16777216)
    end
  end

  describe '#max_message_size' do

    let(:description) do
      described_class.new(replica)
    end

    it 'returns the value' do
      expect(description.max_message_size).to eq(48000000)
    end
  end

  describe '#max_write_batch_size' do

    let(:description) do
      described_class.new(replica)
    end

    it 'returns the value' do
      expect(description.max_write_batch_size).to eq(1000)
    end
  end

  describe '#max_wire_version' do

    context 'when the max wire version is provided' do

      let(:description) do
        described_class.new(replica)
      end

      it 'returns the value' do
        expect(description.max_wire_version).to eq(2)
      end
    end

    context 'when the max wire version is not provided' do

      let(:description) do
        described_class.new({})
      end

      it 'returns the default' do
        expect(description.max_wire_version).to eq(0)
      end
    end
  end

  describe '#min_wire_version' do

    context 'when the min wire version is provided' do

      let(:description) do
        described_class.new(replica)
      end

      it 'returns the value' do
        expect(description.min_wire_version).to eq(0)
      end
    end

    context 'when the min wire version is not provided' do

      let(:description) do
        described_class.new({})
      end

      it 'returns the default' do
        expect(description.min_wire_version).to eq(0)
      end
    end
  end

  describe '#mongos?' do

    context 'when the server is a mongos' do

      let(:config) do
        { 'msg' => 'isdbgrid', 'ismaster' => true }
      end

      let(:description) do
        described_class.new(config)
      end

      it 'returns true' do
        expect(description).to be_mongos
      end
    end

    context 'when the server is not a mongos' do

      let(:description) do
        described_class.new(replica)
      end

      it 'returns false' do
        expect(description).to_not be_mongos
      end
    end
  end

  describe '#passive?' do

    context 'when the server is passive' do

      let(:description) do
        described_class.new({ 'passive' => true })
      end

      it 'returns true' do
        expect(description).to be_passive
      end
    end

    context 'when the server is not passive' do

      let(:description) do
        described_class.new(replica)
      end

      it 'returns false' do
        expect(description).to_not be_passive
      end
    end
  end

  describe '#primary?' do

    context 'when the server is not a primary' do

      let(:description) do
        described_class.new({ 'ismaster' => false })
      end

      it 'returns true' do
        expect(description).to_not be_primary
      end
    end

    context 'when the server is a primary' do

      let(:description) do
        described_class.new(replica)
      end

      it 'returns false' do
        expect(description).to be_primary
      end
    end
  end

  describe '#round_trip_time' do

    let(:description) do
      described_class.new({ 'secondary' => false }, 4.5)
    end

    it 'defaults to 0' do
      expect(described_class.new.round_trip_time).to eq(0)
    end

    it 'can be set via the constructor' do
      expect(description.round_trip_time).to eq(4.5)
    end
  end

  describe '#replica_set_name' do

    context 'when the server is in a replica set' do

      let(:description) do
        described_class.new(replica)
      end

      it 'returns the replica set name' do
        expect(description.replica_set_name).to eq('mongodb_set')
      end
    end

    context 'when the server is not in a replica set' do

      let(:description) do
        described_class.new({})
      end

      it 'returns nil' do
        expect(description.replica_set_name).to be_nil
      end
    end
  end

  describe '#secondary?' do

    context 'when the server is not a secondary' do

      let(:description) do
        described_class.new({ 'secondary' => false })
      end

      it 'returns true' do
        expect(description).to_not be_secondary
      end
    end

    context 'when the server is a secondary' do

      let(:description) do
        described_class.new({ 'secondary' => true, 'setName' => 'test' })
      end

      it 'returns false' do
        expect(description).to be_secondary
      end
    end
  end

  describe '#standalone?' do

    context 'when the server is standalone' do

      let(:description) do
        described_class.new({ 'ismaster' => true })
      end

      it 'returns true' do
        expect(description).to be_standalone
      end
    end

    context 'when the server is part of a replica set' do

      let(:description) do
        described_class.new(replica)
      end

      it 'returns false' do
        expect(description).to_not be_standalone
      end
    end
  end

  describe '#unknown?' do

    context 'when the description has no configuration' do

      let(:description) do
        described_class.new
      end

      it 'returns true' do
        expect(description).to be_unknown
      end
    end

    context 'when the description has a configuration' do

      let(:config) do
        { 'hosts' => [ '127.0.0.1:27019', '127.0.0.1:27020' ] }
      end

      let(:description) do
        described_class.new(config)
      end

      it 'returns false' do
        expect(description).to_not be_unknown
      end
    end
  end
end

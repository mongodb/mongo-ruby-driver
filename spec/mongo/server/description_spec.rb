# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe Mongo::Server::Description do

  %w[ismaster isWritablePrimary].each do |primary_param|
    context "#{primary_param} as primary parameter" do

      let(:replica) do
        {
          'setName' => 'mongodb_set',
          primary_param => true,
          'secondary' => false,
          'hosts' => [
            '127.0.0.1:27018',
            '127.0.0.1:27019'
          ],
          'arbiters' => [
            '127.0.0.1:27120'
          ],
          'primary' => authorized_primary.address.to_s,
          'tags' => { 'rack' => 'a' },
          'me' => '127.0.0.1:27019',
          'maxBsonObjectSize' => 16777216,
          'maxMessageSizeBytes' => 48000000,
          'maxWriteBatchSize' => 1000,
          'maxWireVersion' => 2,
          'minWireVersion' => 1,
          'localTime' => Time.now,
          'lastWrite' => { 'lastWriteDate' => Time.now },
          'logicalSessionTimeoutMinutes' => 7,
          'operationTime' => 1,
          '$clusterTime' => 1,
          'connectionId' => 11,
          'ok' => 1
        }
      end

      let(:address) do
        Mongo::Address.new(authorized_primary.address.to_s)
      end

      let(:monitoring) do
        Mongo::Monitoring.new(monitoring: false)
      end

      declare_topology_double

      let(:cluster) do
        double('cluster').tap do |cl|
          allow(cl).to receive(:topology).and_return(topology)
          allow(cl).to receive(:app_metadata).and_return(app_metadata)
          allow(cl).to receive(:options).and_return({})
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
            expect(description.min_wire_version).to eq(1)
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
            { primary_param => true }
          end

          let(:description) do
            described_class.new(address, config)
          end

          it 'returns an empty hash' do
            expect(description.tags).to eq({})
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

        context 'when the server is a primary' do

          context 'when the hostname contains no capital letters' do

            let(:description) do
              described_class.new(address, replica)
            end

            it 'returns true' do
              expect(description).to be_primary
            end
          end

          context 'when the hostname contains capital letters' do

            let(:description) do
              described_class.new('localhost:27017',
                                  { primary_param => true, 'ok' => 1,
                                    'minWireVersion' => 2, 'maxWireVersion' => 3,
                                    'primary' => 'LOCALHOST:27017',
                                    'setName' => 'itsASet!'})
            end

            it 'returns true' do
              expect(description).to be_primary
            end
          end
        end
      end

      describe '#average_round_trip_time' do

        let(:description) do
          described_class.new(address, { 'secondary' => false }, average_round_trip_time: 4.5)
        end

        it 'defaults to nil' do
          expect(described_class.new(address).average_round_trip_time).to be nil
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

      describe '#server_type' do

        context 'when the server is an arbiter' do

          let(:description) do
            described_class.new(address, { 'arbiterOnly' => true,
                                           'minWireVersion' => 2, 'maxWireVersion' => 3,
                                           'setName' => 'test', 'ok' => 1 })
          end

          it 'returns :arbiter' do
            expect(description.server_type).to eq(:arbiter)
          end
        end

        context 'when the server is a ghost' do

          let(:description) do
            described_class.new(address, { 'isreplicaset' => true,
                                           'minWireVersion' => 2, 'maxWireVersion' => 3, 'ok' => 1 })
          end

          it 'returns :ghost' do
            expect(description.server_type).to eq(:ghost)
          end
        end

        context 'when the server is a mongos' do

          let(:config) do
            { 'msg' => 'isdbgrid', primary_param => true,
              'minWireVersion' => 2, 'maxWireVersion' => 3, 'ok' => 1 }
          end

          let(:description) do
            described_class.new(address, config)
          end

          it 'returns :sharded' do
            expect(description.server_type).to eq(:sharded)
          end

          context 'when client and server addresses are different' do
            let(:config) do
              { 'msg' => 'isdbgrid', primary_param => true,
                'minWireVersion' => 2, 'maxWireVersion' => 3, 'ok' => 1,
                'me' => '127.0.0.1',
              }
            end

            let(:address) do
              Mongo::Address.new('localhost')
            end

            it 'returns :sharded' do
              expect(description.server_type).to eq(:sharded)
            end
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
            described_class.new(address, { 'secondary' => true,
                                           'minWireVersion' => 2, 'maxWireVersion' => 3,
                                           'setName' => 'test', 'ok' => 1 })
          end

          it 'returns :secondary' do
            expect(description.server_type).to eq(:secondary)
          end
        end

        context 'when the server is standalone' do

          let(:description) do
            described_class.new(address, { primary_param => true,
                                           'minWireVersion' => 2, 'maxWireVersion' => 3, 'ok' => 1 })
          end

          it 'returns :standalone' do
            expect(description.server_type).to eq(:standalone)
          end
        end

        context 'when the server is hidden' do

          let(:description) do
            described_class.new(address, { primary_param => false,
                                           'minWireVersion' => 2, 'maxWireVersion' => 3, 'setName' => 'test',
                                           'hidden' => true, 'ok' => 1 })
          end

          it 'returns :other' do
            expect(description.server_type).to eq(:other)
          end
        end

        context 'when the server is other' do

          let(:description) do
            described_class.new(address, { primary_param => false,
                                           'minWireVersion' => 2, 'maxWireVersion' => 3, 'setName' => 'test',
                                           'ok' => 1 })
          end

          it 'returns :other' do
            expect(description.server_type).to eq(:other)
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

      describe '#is_server?' do

        let(:listeners) do
          Mongo::Event::Listeners.new
        end

        let(:server) do
          Mongo::Server.new(address, cluster, monitoring, listeners,
                            monitoring_io: false)
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
            Mongo::Server.new(other_address, cluster, monitoring, listeners,
                              monitoring_io: false)
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
          Mongo::Server.new(server_address, cluster, monitoring, listeners,
                            monitoring_io: false)
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
            { 'msg' => 'isdbgrid', primary_param => true }
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
            described_class.new(address, { primary_param => true,
                                           'minWireVersion' => 2, 'maxWireVersion' => 3, 'ok' => 1 })
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

      describe '#logical_session_timeout_minutes' do

        context 'when a logical session timeout value is in the config' do

          let(:description) do
            described_class.new(address, replica)
          end

          it 'returns the logical session timeout value' do
            expect(description.logical_session_timeout).to eq(7)
          end
        end

        context 'when a logical session timeout value is not in the config' do

          let(:description) do
            described_class.new(address, { primary_param => true,
                                           'minWireVersion' => 2, 'maxWireVersion' => 3, 'ok' => 1 })
          end

          it 'returns nil' do
            expect(description.logical_session_timeout).to be(nil)
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
            'lastWrite' => { 'lastWriteDate' => 1 },
            'operationTime' => 2,
            '$clusterTime' => 2
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

        context 'when the configs match, but have different connectionId values' do

          let(:description) do
            described_class.new(address, replica)
          end

          let(:other) do
            described_class.new(address, replica.merge(
              'connectionId' => 12
            ))
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
            described_class.new(address, { primary_param => true,
                                           'minWireVersion' => 2, 'maxWireVersion' => 3, 'ok' => 1 })
          end

          it 'returns false' do
            expect(description == other).to be(false)
          end
        end

        context 'when one config is a subset of the other' do
          let(:one) do
            described_class.new(address, { primary_param => true,
                                           'minWireVersion' => 2, 'maxWireVersion' => 3, 'ok' => 1 })
          end

          let(:two) do
            described_class.new(address, { primary_param => true,
                                           'minWireVersion' => 2, 'maxWireVersion' => 3,
                                           'ok' => 1, 'setName' => 'mongodb_set' })
          end

          it 'returns false when first config is the receiver' do
            expect(one == two).to be false
          end

          it 'returns false when second config is the receiver' do
            expect(two == one).to be false
          end
        end
      end

      describe '#last_update_time' do
        context 'stub description' do
          let(:description) { described_class.new(address) }

          it 'is present' do
            expect(description.last_update_time).to be_a(Time)
          end
        end

        context 'filled out description' do
          let(:description) { described_class.new(address, replica) }

          it 'is present' do
            expect(description.last_update_time).to be_a(Time)
          end
        end
      end

      describe '#last_update_monotime' do
        context 'stub description' do
          let(:description) { described_class.new(address) }

          it 'is present' do
            expect(description.last_update_monotime).to be_a(Float)
          end
        end

        context 'filled out description' do
          let(:description) { described_class.new(address, replica) }

          it 'is present' do
            expect(description.last_update_monotime).to be_a(Float)
          end
        end
      end
    end

  end
end

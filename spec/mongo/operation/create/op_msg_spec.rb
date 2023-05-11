# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe Mongo::Operation::Create::OpMsg do

  let(:write_concern) do
    Mongo::WriteConcern.get(w: :majority)
  end

  let(:session) { nil }
  let(:spec) do
    { :selector       => { :create => authorized_collection.name },
      :db_name       => authorized_collection.database.name,
      :write_concern => write_concern,
      :session       => session
    }
  end

  let(:op) { described_class.new(spec) }

  let(:connection) do
    double('connection').tap do |connection|
      allow(connection).to receive(:server).and_return(authorized_primary)
      allow(connection).to receive(:features).and_return(authorized_primary.features)
      allow(connection).to receive(:description).and_return(authorized_primary.description)
      allow(connection).to receive(:cluster_time).and_return(authorized_primary.cluster_time)
    end
  end

  describe '#initialize' do

    context 'spec' do

      it 'sets the spec' do
        expect(op.spec).to eq(spec)
      end
    end
  end

  describe '#==' do

    context 'spec' do

      context 'when two ops have the same specs' do
        let(:other) { described_class.new(spec) }

        it 'returns true' do
          expect(op).to eq(other)
        end
      end

      context 'when two ops have different specs' do
        let(:other_selector) do
           { :create => "other_collection_name" }
        end

        let(:other_spec) do
          { :selector       => other_selector,
            :db_name       => authorized_collection.database.name,
            :write_concern => write_concern,
            :ordered       => true
          }
        end
        let(:other) { described_class.new(other_spec) }

        it 'returns false' do
          expect(op).not_to eq(other)
        end
      end
    end
  end

  describe '#selector' do
    min_server_fcv '3.6'

    it 'does not mutate user input' do
      user_input = IceNine.deep_freeze(spec.dup)
      expect do
        described_class.new(user_input).send(:selector, connection)
      end.not_to raise_error
    end
  end

  describe '#message' do
    # https://jira.mongodb.org/browse/RUBY-2224
    require_no_linting

    context 'when the server supports OP_MSG' do

      let(:global_args) do
        {
            create: TEST_COLL,
            writeConcern: write_concern.options,
            '$db' => SpecConfig.instance.test_db,
            lsid: session.session_id
        }
      end

      let(:session) do
        authorized_client.start_session
      end

      context 'when the topology is replica set or sharded' do
        min_server_fcv '3.6'
        require_topology :replica_set, :sharded

        let(:expected_global_args) do
          global_args.merge(Mongo::Operation::CLUSTER_TIME => authorized_client.cluster.cluster_time)
        end

        it 'creates the correct OP_MSG message' do
          authorized_client.command(ping:1)
          expect(Mongo::Protocol::Msg).to receive(:new).with([], {}, expected_global_args)
          op.send(:message, connection)
        end
      end

      context 'when the topology is standalone' do
        min_server_fcv '3.6'
        require_topology :single

        let(:expected_global_args) do
          global_args
        end

        it 'creates the correct OP_MSG message' do
          authorized_client.command(ping:1)
          expect(Mongo::Protocol::Msg).to receive(:new).with([], {}, expected_global_args)
          op.send(:message, connection)
        end

        context 'when an implicit session is created and the topology is then updated and the server does not support sessions' do
          # Mocks on features are incompatible with linting
          require_no_linting

          let(:expected_global_args) do
            global_args.dup.tap do |args|
              args.delete(:lsid)
            end
          end

          let(:session) do
            Mongo::Session.new(nil, authorized_client, implicit: true).tap do |session|
              allow(session).to receive(:session_id).and_return(42)
              session.should be_implicit
            end
          end

          it 'creates the correct OP_MSG message' do
            RSpec::Mocks.with_temporary_scope do
              expect(connection.features).to receive(:sessions_enabled?).and_return(false)

              expect(expected_global_args[:session]).to be nil
              expect(Mongo::Protocol::Msg).to receive(:new).with([], {}, expected_global_args)
              op.send(:message, connection)
            end
          end
        end
      end

      context 'when the write concern is 0' do

        let(:write_concern) do
          Mongo::WriteConcern.get(w: 0)
        end

        context 'when the session is implicit' do

          let(:session) do
            Mongo::Session.new(nil, authorized_client, implicit: true).tap do |session|
              allow(session).to receive(:session_id).and_return(42)
              session.should be_implicit
            end
          end

          context 'when the topology is replica set or sharded' do
            min_server_fcv '3.6'
            require_topology :replica_set, :sharded

            let(:expected_global_args) do
              global_args.dup.tap do |args|
                args.delete(:lsid)
                args.merge!(Mongo::Operation::CLUSTER_TIME => authorized_client.cluster.cluster_time)
              end
            end

            it 'does not send a session id in the command' do
              authorized_client.command(ping:1)
              expect(Mongo::Protocol::Msg).to receive(:new).with([:more_to_come], {}, expected_global_args)
              op.send(:message, connection)
            end
          end

          context 'when the topology is standalone' do
            min_server_fcv '3.6'
            require_topology :single

            let(:expected_global_args) do
              global_args.dup.tap do |args|
                args.delete(:lsid)
              end
            end

            it 'creates the correct OP_MSG message' do
              authorized_client.command(ping:1)
              expect(Mongo::Protocol::Msg).to receive(:new).with([:more_to_come], {}, expected_global_args)
              op.send(:message, connection)
            end
          end
        end

        context 'when the session is explicit' do
          min_server_fcv '3.6'
          require_topology :replica_set, :sharded

          let(:session) do
            authorized_client.start_session
          end

          before do
            session.should_not be_implicit
          end

          let(:expected_global_args) do
            global_args.dup.tap do |args|
              args.delete(:lsid)
              args.merge!(Mongo::Operation::CLUSTER_TIME => authorized_client.cluster.cluster_time)
            end
          end

          it 'does not send a session id in the command' do
            authorized_client.command(ping:1)
            RSpec::Mocks.with_temporary_scope do
              expect(Mongo::Protocol::Msg).to receive(:new).with([:more_to_come], {}, expected_global_args)
              op.send(:message, connection)
            end
          end
        end
      end
    end
  end
end

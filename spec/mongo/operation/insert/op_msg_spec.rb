# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe Mongo::Operation::Insert::OpMsg do

  let(:documents) { [{ :_id => 1, :foo => 1 }] }
  let(:session) { nil }
  let(:spec) do
    { :documents     => documents,
      :db_name       => authorized_collection.database.name,
      :coll_name     => authorized_collection.name,
      :write_concern => write_concern,
      :ordered       => true,
      :session       => session
    }
  end

  let(:write_concern) do
    Mongo::WriteConcern.get(w: :majority)
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
        let(:other_documents) { [{ :bar => 1 }] }
        let(:other_spec) do
          { :documents     => other_documents,
            :db_name       => authorized_collection.database.name,
            :insert        => authorized_collection.name,
            :write_concern => write_concern.options,
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

  describe 'write concern' do
    # https://jira.mongodb.org/browse/RUBY-2224
    require_no_linting

    context 'when write concern is not specified' do

      let(:spec) do
        { :documents     => documents,
          :db_name       => authorized_collection.database.name,
          :coll_name     => authorized_collection.name,
          :ordered       => true
        }
      end

      it 'does not include write concern in the selector' do
        expect(op.send(:command, connection)[:writeConcern]).to be_nil
      end
    end

    context 'when write concern is specified' do

      it 'includes write concern in the selector' do
        expect(op.send(:command, connection)[:writeConcern]).to eq(BSON::Document.new(write_concern.options))
      end
    end
  end

  describe '#message' do
    # https://jira.mongodb.org/browse/RUBY-2224
    require_no_linting

    context 'when the server supports OP_MSG' do
      min_server_fcv '3.6'

      let(:documents) do
        [ { foo: 1 }, { bar: 2 }]
      end

      let(:global_args) do
        {
            insert: TEST_COLL,
            ordered: true,
            writeConcern: write_concern.options,
            '$db' => SpecConfig.instance.test_db,
            lsid: session.session_id
        }
      end

      let!(:expected_payload_1) do
        Mongo::Protocol::Msg::Section1.new('documents', op.documents)
      end

      let(:session) do
        Mongo::Session.new(nil, authorized_client, implicit: true).tap do |session|
          allow(session).to receive(:session_id).and_return(42)
        end
      end

      context 'when the topology is replica set or sharded' do
        min_server_fcv '3.6'
        require_topology :replica_set, :sharded

        let(:expected_global_args) do
          global_args.merge(Mongo::Operation::CLUSTER_TIME => authorized_client.cluster.cluster_time)
        end

        it 'creates the correct OP_MSG message' do
          authorized_client.command(ping:1)
          RSpec::Mocks.with_temporary_scope do
            expect(Mongo::Protocol::Msg).to receive(:new).with([],
                                                               {},
                                                               expected_global_args,
                                                               expected_payload_1)
            op.send(:message, connection)
          end
        end
      end

      context 'when the topology is standalone' do
        min_server_fcv '3.6'
        require_topology :single

        let(:expected_global_args) do
          global_args
        end

        it 'creates the correct OP_MSG message' do
          RSpec::Mocks.with_temporary_scope do
            authorized_client.command(ping:1)
            expect(Mongo::Protocol::Msg).to receive(:new).with([],
                                                               {},
                                                               expected_global_args,
                                                               expected_payload_1)
            op.send(:message, connection)
          end
        end

        context 'when an implicit session is created and the topology is then updated and the server does not support sessions' do
          # Mocks on features are incompatible with linting
          require_no_linting

          let(:expected_global_args) do
            global_args.dup.tap do |args|
              args.delete(:lsid)
            end
          end

          before do
            session.implicit?.should be true
          end

          it 'creates the correct OP_MSG message' do
            RSpec::Mocks.with_temporary_scope do
              expect(connection.features).to receive(:sessions_enabled?).and_return(false)

              expect(expected_global_args).not_to have_key(:lsid)
              expect(Mongo::Protocol::Msg).to receive(:new).with([],
                                                                 {},
                                                                 expected_global_args,
                                                                 expected_payload_1)
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
              RSpec::Mocks.with_temporary_scope do
                expect(Mongo::Protocol::Msg).to receive(:new).with([:more_to_come],
                                                                   {},
                                                                   expected_global_args,
                                                                   expected_payload_1)
                op.send(:message, connection)
              end
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
              RSpec::Mocks.with_temporary_scope do
                expect(Mongo::Protocol::Msg).to receive(:new).with([:more_to_come],
                                                                   {},
                                                                   expected_global_args,
                                                                   expected_payload_1)
                op.send(:message, connection)
              end
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
              expect(Mongo::Protocol::Msg).to receive(:new).with([:more_to_come],
                                                                 {},
                                                                 expected_global_args,
                                                                 expected_payload_1)
              op.send(:message, connection)
            end
          end
        end
      end
    end
  end
end

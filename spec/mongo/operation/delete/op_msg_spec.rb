require 'spec_helper'

describe Mongo::Operation::Delete::OpMsg do

  let(:write_concern) do
    Mongo::WriteConcern.get(w: :majority)
  end

  let(:session) { nil }
  let(:deletes) { [{:q => { :foo => 1 }, :limit => 1}] }
  let(:spec) do
    { :deletes       => deletes,
      :db_name       => authorized_collection.database.name,
      :coll_name     => authorized_collection.name,
      :write_concern => write_concern,
      :ordered       => true,
      :session       => session
    }
  end

  let(:op) { described_class.new(spec) }

  let(:connection) do
    double('connection').tap do |connection|
      allow(connection).to receive(:server).and_return(authorized_primary)
      allow(connection).to receive(:features).and_return(authorized_primary.features)
      allow(connection).to receive(:standalone?).and_return(authorized_primary.standalone?)
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
        let(:other_deletes) { [{:q => { :bar => 1 }, :limit => 1}] }
        let(:other_spec) do
          { :deletes       => other_deletes,
            :db_name       => authorized_collection.database.name,
            :coll_name     => authorized_collection.name,
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

  describe 'write concern' do
    # https://jira.mongodb.org/browse/RUBY-2224
    skip_if_linting

    context 'when write concern is not specified' do

      let(:spec) do
        { :deletes       => deletes,
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
        expect(op.send(:command, connection)[:writeConcern]).to eq(write_concern.options)
      end
    end
  end

  describe '#message' do
    # https://jira.mongodb.org/browse/RUBY-2224
    skip_if_linting

    context 'when the server supports OP_MSG' do

      let(:global_args) do
        {
            delete: TEST_COLL,
            ordered: true,
            writeConcern: write_concern.options,
            '$db' => SpecConfig.instance.test_db,
            lsid: session.session_id
        }
      end

      let(:expected_payload_1) do
        Mongo::Protocol::Msg::Section1.new('deletes', deletes)
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
          expect(Mongo::Protocol::Msg).to receive(:new).with([], {}, expected_global_args, expected_payload_1)
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
          expect(Mongo::Protocol::Msg).to receive(:new).with([], {}, expected_global_args, expected_payload_1)
          op.send(:message, connection)
        end

        context 'when an implicit session is created and the topology is then updated and the server does not support sessions' do
          # Mocks on features are incompatible with linting
          skip_if_linting

          let(:expected_global_args) do
            global_args.dup.tap do |args|
              args.delete(:lsid)
            end
          end

          before do
            session.instance_variable_set(:@options, { implicit: true })
          end

          it 'creates the correct OP_MSG message' do
            RSpec::Mocks.with_temporary_scope do
              expect(connection.features).to receive(:sessions_enabled?).and_return(false)

              expect(expected_global_args[:session]).to be nil
              expect(Mongo::Protocol::Msg).to receive(:new).with([], {}, expected_global_args, expected_payload_1)
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
            # Use client#get_session so the session is implicit
            authorized_client.send(:get_session)
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
              expect(Mongo::Protocol::Msg).to receive(:new).with([:more_to_come], {}, expected_global_args, expected_payload_1)
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
              expect(Mongo::Protocol::Msg).to receive(:new).with([:more_to_come], {}, expected_global_args, expected_payload_1)
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

          let(:expected_global_args) do
            global_args.dup.tap do |args|
              args.delete(:lsid)
              args.merge!(Mongo::Operation::CLUSTER_TIME => authorized_client.cluster.cluster_time)
            end
          end

          it 'does not send a session id in the command' do
            authorized_client.command(ping:1)
            RSpec::Mocks.with_temporary_scope do
              expect(Mongo::Protocol::Msg).to receive(:new).with([:more_to_come], {}, expected_global_args, expected_payload_1)
              op.send(:message, connection)
            end
          end
        end
      end
    end
  end
end

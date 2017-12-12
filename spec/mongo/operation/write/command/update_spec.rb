require 'spec_helper'

describe Mongo::Operation::Write::Command::Update do

  let(:updates) { [{:q => { :foo => 1 },
                    :u => { :$set => { :bar => 1 } },
                    :multi => true,
                    :upsert => false }] }

  let(:write_concern) do
    Mongo::WriteConcern.get(WRITE_CONCERN)
  end
  let(:session) { nil }
  let(:spec) do
    { :updates       => updates,
      :db_name       => TEST_DB,
      :coll_name     => TEST_COLL,
      :write_concern => write_concern,
      :ordered       => true,
      :session       => session
    }
  end

  let(:op) { described_class.new(spec) }

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
        let(:other_updates) { [{:q => { :bar => 1 },
                          :u => { :$set => { :bar => 2 } },
                          :multi => true,
                          :upsert => false }] }
        let(:other_spec) do
          { :updates       => other_updates,
            :db_name       => TEST_DB,
            :coll_name     => TEST_COLL,
            :write_concern => Mongo::WriteConcern.get(WRITE_CONCERN),
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

    context 'when write concern is not specified' do

      let(:spec) do
        { :updates       => updates,
          :db_name       => TEST_DB,
          :coll_name     => TEST_COLL,
          :ordered       => true
        }
      end

      it 'does not include write concern in the selector' do
        expect(op.send(:selector)[:writeConcern]).to be_nil
      end
    end

    context 'when write concern is specified' do

      it 'includes write concern in the selector' do
        expect(op.send(:selector)[:writeConcern]).to eq(write_concern.options)
      end
    end
  end

  describe '#message' do

    context 'when the server supports OP_MSG', if: op_msg_enabled? do

      let(:global_args) do
        {
            update: TEST_COLL,
            ordered: true,
            writeConcern: write_concern.options,
            '$db' => TEST_DB,
            lsid: session.session_id
        }
      end

      let(:expected_payload_1) do
        {
          type: 1,
          payload: { identifier: 'updates',
                     sequence: updates
          }
        }
      end

      let(:session) do
        authorized_client.start_session
      end

      context 'when the topology is replica set or sharded', if: (replica_set? || sharded?) && op_msg_enabled? do

        let(:expected_global_args) do
          global_args.merge(Mongo::Operation::CLUSTER_TIME => authorized_client.cluster.cluster_time)
        end

        it 'creates the correct OP_MSG message' do
          authorized_client.command(ping:1)
          expect(Mongo::Protocol::Msg).to receive(:new).with([:none], {}, expected_global_args, expected_payload_1)
          op.send(:message, authorized_primary)
        end
      end

      context 'when the topology is standalone', if: standalone? && op_msg_enabled? do

        let(:expected_global_args) do
          global_args
        end

        it 'creates the correct OP_MSG message' do
          authorized_client.command(ping:1)
          expect(Mongo::Protocol::Msg).to receive(:new).with([:none], {}, expected_global_args, expected_payload_1)
          op.send(:message, authorized_primary)
        end

        context 'when an implicit session is created and the topology is then updated and the server does not support sessions' do

          let(:expected_global_args) do
            global_args.delete(:lsid)
            global_args
          end

          before do
            session.instance_variable_set(:@options, { implicit: true })
            allow(authorized_primary.features).to receive(:sessions_enabled?).and_return(false)
          end

          it 'creates the correct OP_MSG message' do
            authorized_client.command(ping:1)
            expect(Mongo::Protocol::Msg).to receive(:new).with([:none], {}, expected_global_args, expected_payload_1)
            op.send(:message, authorized_primary)
          end
        end
      end

      context 'when the write concern is 0' do

        let(:write_concern) do
          Mongo::WriteConcern.get(w: 0)
        end

        context 'when the topology is replica set or sharded', if: (replica_set? || sharded?) && op_msg_enabled? do

          let(:expected_global_args) do
            global_args.delete(:lsid)
            global_args.merge(Mongo::Operation::CLUSTER_TIME => authorized_client.cluster.cluster_time)
          end

          it 'creates the correct OP_MSG message' do
            authorized_client.command(ping:1)
            expect(Mongo::Protocol::Msg).to receive(:new).with([:more_to_come], {}, expected_global_args, expected_payload_1)
            op.send(:message, authorized_primary)
          end
        end

        context 'when the topology is standalone', if: standalone? && op_msg_enabled? do

          let(:expected_global_args) do
            global_args.delete(:lsid)
            global_args
          end

          it 'creates the correct OP_MSG message' do
            authorized_client.command(ping:1)
            expect(Mongo::Protocol::Msg).to receive(:new).with([:more_to_come], {}, expected_global_args, expected_payload_1)
            op.send(:message, authorized_primary)
          end
        end
      end
    end

    context 'when the server does not support OP_MSG' do

      let(:expected_selector) do
        {
            :update        => TEST_COLL,
            :updates       => updates,
            :ordered       => true,
            :writeConcern   => write_concern.options
        }
      end

      it 'creates the correct Command message', unless: op_msg_enabled? do
        expect(Mongo::Protocol::Query).to receive(:new).with(TEST_DB, '$cmd', expected_selector, { limit: -1 })
        op.send(:message, authorized_primary)
      end
    end
  end
end

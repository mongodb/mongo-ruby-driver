require 'spec_helper'

describe Mongo::Operation::Write::Command::Update do

  let(:updates) { [{:q => { :foo => 1 },
                    :u => { :$set => { :bar => 1 } },
                    :multi => true,
                    :upsert => false }] }

  let(:write_concern) do
    Mongo::WriteConcern.get(WRITE_CONCERN)
  end

  let(:spec) do
    { :updates       => updates,
      :db_name       => TEST_DB,
      :coll_name     => TEST_COLL,
      :write_concern => write_concern,
      :ordered       => true
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

      let(:expected_payload_0) do
        {
          type: 0,
          document: {
              update: TEST_COLL,
              ordered: true,
              writeConcern: write_concern.options,
              '$db' => TEST_DB
          }
        }
      end

      let(:expected_payload_1) do
        {
          type: 1,
          sequence: { identifier: 'updates',
                      documents: updates
          }
        }
      end

      it 'creates the correct OP_MSG message' do
        expect(Mongo::Protocol::Msg).to receive(:new).with([:none], {}, expected_payload_0, expected_payload_1)
        op.send(:message, authorized_primary)
      end

      context 'when the write concern is 0' do

        let(:write_concern) do
          Mongo::WriteConcern.get(w: 0)
        end

        it 'creates the correct OP_MSG message' do
          expect(Mongo::Protocol::Msg).to receive(:new).with([:more_to_come], {}, expected_payload_0, expected_payload_1)
          op.send(:message, authorized_primary)
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

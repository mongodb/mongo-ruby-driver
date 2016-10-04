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

    let(:expected_selector) do
      {
        :update        => TEST_COLL,
        :updates       => updates,
        :ordered       => true,
        :writeConcern   => write_concern.options
      }
    end

    it 'creates the correct Command message' do
      expect(Mongo::Protocol::Query).to receive(:new).with(TEST_DB, '$cmd', expected_selector, { limit: -1 })
      op.send(:message, double('server'))
    end
  end
end

require 'spec_helper'

describe Mongo::Operation::Update::Command do

  let(:updates) { [{:q => { :foo => 1 },
                    :u => { :$set => { :bar => 1 } },
                    :multi => true,
                    :upsert => false }] }

  let(:write_concern) do
    Mongo::WriteConcern.get(w: :majority)
  end
  let(:session) { nil }
  let(:spec) do
    { :updates       => updates,
      :db_name       => SpecConfig.instance.test_db,
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
            :db_name       => SpecConfig.instance.test_db,
            :coll_name     => TEST_COLL,
            :write_concern => Mongo::WriteConcern.get(w: :majority),
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
        { :updates       => updates,
          :db_name       => SpecConfig.instance.test_db,
          :coll_name     => TEST_COLL,
          :ordered       => true
        }
      end

      it 'does not include write concern in the selector' do
        expect(op.send(:command, double('server'))[:writeConcern]).to be_nil
      end
    end

    context 'when write concern is specified' do

      it 'includes write concern in the selector' do
        expect(op.send(:command, double('server'))[:writeConcern]).to eq(write_concern.options)
      end
    end
  end

  describe '#message' do
    # https://jira.mongodb.org/browse/RUBY-2224
    skip_if_linting

    context 'when the server does not support OP_MSG' do
      max_server_version '3.4'

      let(:expected_selector) do
        {
            :update        => TEST_COLL,
            :updates       => updates,
            :ordered       => true,
            :writeConcern   => write_concern.options
        }
      end

      it 'creates the correct Command message' do
        expect(Mongo::Protocol::Query).to receive(:new).with(SpecConfig.instance.test_db, '$cmd', expected_selector, { limit: -1 })
        op.send(:message, authorized_primary)
      end
    end
  end
end

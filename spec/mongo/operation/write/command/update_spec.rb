require 'spec_helper'

describe Mongo::Operation::Write::Command::Update do
  include_context 'operation'

  let(:updates) { [{:q => { :foo => 1 },
                    :u => { :$set => { :bar => 1 } },
                    :multi => true,
                    :upsert => false }] }
  let(:spec) do
    { :updates       => updates,
      :db_name       => db_name,
      :coll_name     => coll_name,
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
            :db_name       => db_name,
            :coll_name     => coll_name,
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

  context '#merge' do
    let(:other_op) { described_class.new(spec) }

    it 'is not allowed' do
      expect{ op.merge(other_op) }.to raise_exception
    end
  end

  context '#merge!' do
    let(:other_op) { described_class.new(spec) }

    it 'is not allowed' do
      expect{ op.merge!(other_op) }.to raise_exception
    end
  end

  describe '#execute' do

    context 'server' do

      context 'message' do
        let(:expected_selector) do
          { :updates       => updates,
            :update        => coll_name,
            :writeConcern => write_concern.options,
            :ordered       => true
          }
        end

        it 'creates a query wire protocol message with correct specs' do
          allow_any_instance_of(Mongo::ServerPreference::Primary).to receive(:server) do
            primary_server
          end

          expect(Mongo::Protocol::Query).to receive(:new) do |db, coll, sel, options|
            expect(db).to eq(db_name)
            expect(coll).to eq(Mongo::Database::COMMAND)
            expect(sel).to eq(expected_selector)
          end
          op.execute(primary_context)
        end
      end

      context 'write concern' do

        context 'w == 0' do

          pending 'no response is returned'
        end

        context 'w > 0' do

          pending 'returns a response'
        end
      end
    end
  end
end


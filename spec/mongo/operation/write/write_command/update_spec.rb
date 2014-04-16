require 'spec_helper'

describe Mongo::Operation::Write::WriteCommand::Update do
  include_context 'operation'

  let(:updates) { [{:q => { :foo => 1 },
                    :u => { :$set => { :bar => 1 } },
                    :multi => true,
                    :upsert => false }] }
  let(:spec) do
    { :updates       => updates,
      :db_name       => 'test',
      :coll_name     => 'test_coll',
      :write_concern => { 'w' => 1 },
      :ordered       => true
    }
  end

  let(:context) { {} }
  let(:op) { described_class.new(spec, context) }

  describe '#initialize' do

    context 'spec' do

      it 'sets the spec' do
        expect(op.spec).to eq(spec)
      end
    end

    context 'server' do

      context 'when a server is provided' do
        let(:context) { { :server => server } }

        it 'sets the server' do
          expect(op.context[:server]).to eq(server)
        end
      end

      context 'when a server is not provided' do
        let(:context) { { } }

        it 'does not set a server' do
          expect(op.context[:server]).to be_nil
        end
      end
    end
  end

  describe '#==' do

    context 'context' do

      context 'when two ops have the same context' do
        let(:other) { described_class.new(spec, context) }

        it 'returns true' do
          expect(op).to eq(other)
        end
      end

      context 'when two ops have a different context' do

        context 'different servers' do
          let(:context) { { :server => server } }
          let(:other_server) { double('server') }
          let(:other_context) { { :server => other_server } }
          let(:other) { described_class.new(spec, other_context) }

          it 'returns false' do
            expect(op).not_to eq(other)
          end
        end
      end
    end

    context 'spec' do

      context 'when two ops have the same specs' do
        let(:other) { described_class.new(spec, context) }

        it 'returns true' do
          expect(op).to eq(other)
        end
      end

      context 'when two ops have different specs' do
        let(:other_updates) { [{:q => { :foo => 1 },
                                :u => { :$set => { :bar => 1 } },
                                :multi => true,
                                :upsert => true }] }
        let(:other_spec) do
          { :updates       => other_updates,
            :db_name       => 'test',
            :coll_name     => 'test_coll',
            :write_concern => { 'w' => 1 },
            :ordered       => true
          }
        end
        let(:other) { described_class.new(other_spec, context) }

        it 'returns false' do
          expect(op).not_to eq(other)
        end
      end
    end
  end

  describe '#context' do

    context 'preference' do

      it 'includes the primary server preference' do
        expect(op.context[:server_preference]).to eq(Mongo::ServerPreference.get(:primary))
      end
    end

    context 'when a server is provided' do
      let(:context) { { :server => server } }

      it 'includes the server' do
        expect(op.context[:server]).to eq(server)
      end
    end
  end

  describe '#execute' do

    context 'send' do
      it 'sends the write command' do

      end
    end

    context 'write concern' do

      context 'w == 0' do

        it 'no response is returned' do

        end
      end

      context 'w > 0' do

        it 'returns a response' do

        end
      end
    end
  end
end


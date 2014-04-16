require 'spec_helper'

describe Mongo::Operation::Command do
  include_context 'operation'

  let(:server_pref) { Mongo::ServerPreference.get(:primary) }
  let(:selector) { { :ismaster => 1 } }
  let(:spec) do
    { :selector => selector,
      :opts => {},
      :db_name => 'admin'
    }
  end
  let(:context) { {} }
  let(:op) { described_class.new(spec, context) }

  describe '#initialize' do

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

    context 'spec' do

      it 'sets the spec' do
        expect(op.spec).to be(spec)
      end
    end

    context 'read' do

      context 'read preference is specified' do
        let(:context) { { :server_preference => server_pref } }

        it 'sets the read pref' do
          expect(op.context[:server_preference]).to be(server_pref)
        end
      end

      context 'read preference is not specified' do
        let(:context) { { } }

        it 'uses a primary read preference' do
          expect(op.context[:server_preference]).to eq(Mongo::ServerPreference.get(:primary))
        end
      end
    end
  end

  describe '#==' do

    context 'when two ops have the same context' do
      let(:other) { described_class.new(spec, context) }

      it 'returns true' do
        expect(op).to eq(other)
      end
    end

    context 'when two ops have a different context' do

      context 'different read pref' do
        let(:context) { { :server_preference => server_pref, :server => server } }
        let(:other_server_pref) { Mongo::ServerPreference.get(:secondary) }
        let(:other_context) { { :server_preference => other_server_pref, :server => server } }
        let(:other) { described_class.new(spec, other_context) }

        it 'returns false' do
          expect(op).not_to eq(other)
        end
      end

      context 'different servers' do
        let(:context) { { :server_preference => server_pref, :server => server } }
        let(:other_server) { double('server') }
        let(:other_context) { { :server_preference => server_pref, :server => other_server } }
        let(:other) { described_class.new(spec, other_context) }

        it 'returns false' do
          expect(op).not_to eq(other)
        end
      end
    end

    context ' when two ops have different specs' do
      let(:other_selector) { { :ping => 1 } }
      let(:other_spec) do
        { :selector => other_selector,
          :opts => {},
          :db_name => 'test',
        }
      end
      let(:other) { described_class.new(other_spec, context) }

      it 'returns false' do
        expect(op).not_to eq(other)
      end
    end
  end

  describe '#context' do

    context 'when read preference is provided' do

      context 'not primary' do
        let(:server_pref) { Mongo::ServerPreference.get(:secondary) }
        let(:context) { { :server_preference => server_pref } }

        context 'secondary is allowed' do

          it 'uses the provided read preference' do
            expect(op.context[:server_preference]).to be(server_pref)
          end
        end

        context 'secondary is not allowed' do
          let(:selector) { { :listDatabases => 1 } }

          it 'reroutes to primary and warns' do
            # @todo: expect warning
            expect(op.context[:server_preference].name).to eq(:primary)
          end
        end
      end

      context 'primary' do
        let(:server_pref) { Mongo::ServerPreference.get(:primary) }
        let(:context) { { :server_preference => server_pref } }

        it 'uses the user-provided read preference' do
          expect(op.context[:server_preference]).to be(server_pref)
        end
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
    # @todo
  end
end

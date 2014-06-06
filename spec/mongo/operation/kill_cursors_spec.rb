require 'spec_helper'

describe Mongo::Operation::KillCursors do
  include_context 'operation'

  # selector
  let(:spec) { { :cursor_ids => [1,2] } }

  let(:context) { { :server => server } }
  let(:op) { described_class.new(spec, context) }

  describe '#initialize' do

    context 'server' do

      context 'when a server is provided' do

        it 'sets the server' do
          expect(op.context[:server]).to eq(server)
        end
      end

      context 'when a server is not provided' do
        let(:context) { { } }

        it 'raises an exception' do
          # @todo: add specific exception
          expect{ op }.to raise_exception
        end
      end
    end

    context 'connection' do

      context 'connection is specified' do
        let(:context) { { :server => server, :connection => connection } }

        it 'sets the connection' do
          expect(op.context[:connection]).to be(connection)
        end
      end

      context 'connection is not specified' do
        let(:context) { { :server => server } }

        it 'does not set the connection' do
          expect(op.context[:connection]).to be_nil
        end
      end
    end

    it 'sets the spec' do
      expect(op.spec).to be(spec)
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

      context 'different server' do
        let(:other_server) { double('server') }
        let(:other_context) { { :server => other_server } }
        let(:other) { described_class.new(spec, other_context) }

        it 'returns false' do
          expect(op).not_to eq(other)
        end
      end

      context 'different connections' do
        let(:context) { { :server => server, :connection => connection } }
        let(:other_conn) { double('connection') }
        let(:other_context) { { :server => server, :connection => other_conn } }
        let(:other) { described_class.new(spec, other_context) }

        it 'returns false' do
          expect(op).not_to eq(other)
        end
      end
    end

    context ' when two ops have different specs' do
      let(:other_spec) do
        { :cursor_ids => [1, 2, 3] }
      end
      let(:other) { described_class.new(other_spec, context) }

      it 'returns false' do
        expect(op).not_to eq(other)
      end
    end
  end

  describe '#execute' do
    # @todo: what will the connection#send_and_receive API be like?
  end
end

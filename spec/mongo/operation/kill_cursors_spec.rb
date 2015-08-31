require 'spec_helper'

describe Mongo::Operation::KillCursors do
  include_context 'operation'

  let(:spec) { { :cursor_ids => [1,2] } }
  let(:op) { described_class.new(spec) }

  describe '#initialize' do

    it 'sets the spec' do
      expect(op.spec).to be(spec)
    end
  end

  describe '#==' do

    context ' when two ops have different specs' do
      let(:other_spec) do
        { :cursor_ids => [1, 2, 3] }
      end
      let(:other) { described_class.new(other_spec) }

      it 'returns false' do
        expect(op).not_to eq(other)
      end
    end
  end

  describe '#execute' do

    context 'message' do

      it 'creates a kill cursors wire protocol message with correct specs' do
        expect(Mongo::Protocol::KillCursors).to receive(:new) do |collection, database, ids|
          expect(ids).to eq(spec[:cursor_ids])
        end
        op.execute(primary_context)
      end
    end

    context 'connection' do

      it 'dispatches the message on the connection' do
        expect(connection).to receive(:dispatch)
        op.execute(primary_context)
      end
    end
  end
end

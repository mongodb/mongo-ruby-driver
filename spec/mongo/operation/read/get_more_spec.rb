require 'spec_helper'

describe Mongo::Operation::Read::GetMore do
  include_context 'operation'

  let(:to_return) { 50 }
  let(:cursor_id) { 1 }

  let(:spec) do
    { :db_name   => db_name,
      :coll_name => coll_name,
      :to_return => to_return,
      :cursor_id => cursor_id }
  end

  let(:op) { described_class.new(spec) }

  describe '#initialize' do

    it 'sets the spec' do
      expect(op.spec).to be(spec)
    end
  end

  describe '#==' do

    context ' when two ops have different specs' do
      let(:other_spec) do
        { :db_name   => 'test_db',
          :coll_name => 'test_coll',
          :to_return => 50,
          :cursor_id => 2 }
      end
      let(:other) { described_class.new(other_spec) }

      it 'returns false' do
        expect(op).not_to eq(other)
      end
    end
  end

  describe '#execute' do

    context 'message' do

      it 'creates a get more wire protocol message with correct specs' do
        expect(Mongo::Protocol::GetMore).to receive(:new) do |db, coll, to_ret, id|
          expect(db).to eq(db_name)
          expect(coll).to eq(coll_name)
          expect(to_ret).to eq(to_return)
          expect(id).to eq(cursor_id)
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

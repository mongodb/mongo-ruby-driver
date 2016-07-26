require 'spec_helper'

describe Mongo::Operation::Read::GetMore do

  let(:to_return) do
    50
  end

  let(:cursor_id) do
    1
  end

  let(:spec) do
    { :db_name   => TEST_DB,
      :coll_name => TEST_COLL,
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

  describe '#message' do

    it 'creates a get more wire protocol message with correct specs' do
      expect(Mongo::Protocol::GetMore).to receive(:new).with(TEST_DB, TEST_COLL, to_return, cursor_id)
      op.send(:message, authorized_primary)
    end
  end
end

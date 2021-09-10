# frozen_string_literal: true
# encoding: utf-8

require 'spec_helper'

describe Mongo::Operation::KillCursors::Legacy do

  let(:spec) do
    { coll_name: TEST_COLL,
      db_name: SpecConfig.instance.test_db,
      :cursor_ids => [1,2]
    }
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
        { :cursor_ids => [1, 2, 3] }
      end
      let(:other) { described_class.new(other_spec) }

      it 'returns false' do
        expect(op).not_to eq(other)
      end
    end
  end

  describe '#message' do
    let(:expected_cursor_ids) do
      spec[:cursor_ids].map { |v| BSON::Int64.new(v) }
    end

    it 'creates a kill cursors wire protocol message with correct specs' do
      expect(Mongo::Protocol::KillCursors).to receive(:new).with(TEST_COLL, SpecConfig.instance.test_db, expected_cursor_ids)
      op.send(:message, double('server'))
    end
  end
end

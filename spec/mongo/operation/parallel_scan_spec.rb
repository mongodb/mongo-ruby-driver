require 'spec_helper'

describe Mongo::Operation::ParallelScan do

  let(:spec) do
    { :cursor_count => 5,
      :coll_name => TEST_COLL,
      :db_name => TEST_DB
    }
  end

  let(:op) do
    described_class.new(spec)
  end

  describe '#execute' do

    let(:documents) do
      [
        { name: 'Berlin' },
        { name: 'London' }
      ]
    end

    before do
      authorized_collection.insert_many(documents)
    end

    after do
      authorized_collection.find.remove_many
    end

    let(:result) do
      op.execute(authorized_primary.context)
    end

    it 'returns the parallel scan result', if: write_command_enabled? do
      expect(result.cursor_ids).to_not be_empty
    end

    it 'raises an error', unless: write_command_enabled? do
      expect {
        result.cursor_ids
      }.to raise_error(Mongo::Operation::Write::Failure)
    end
  end
end

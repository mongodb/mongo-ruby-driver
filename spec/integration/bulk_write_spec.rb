require 'spec_helper'

describe 'Bulk writes' do
  before do
    authorized_collection.drop
  end

  context 'when bulk write is larger than 48MB' do
    let(:operations) do
      [ { insert_one: { text: 'a' * 1000 * 1000 } } ] * 48
    end

    it 'succeeds' do
      expect do
        authorized_collection.bulk_write(operations)
      end.not_to raise_error
    end
  end
end

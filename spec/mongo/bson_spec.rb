require 'spec_helper'

describe Symbol do

  describe '#bson_type' do

    it 'serializes to a symbol type' do
      expect(:test.bson_type).to eq(14.chr)
    end
  end
end

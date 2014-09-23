require 'spec_helper'

describe Mongo::Grid::FS do

  describe '#initialize' do

    let(:fs) do
      described_class.new(authorized_client.database)
    end

    it 'sets the database' do
      expect(fs.database).to eq(authorized_client.database)
    end
  end
end

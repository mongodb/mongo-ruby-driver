require 'spec_helper'

describe Mongo::Operation::Commands::CollectionsInfo do

  let(:spec) do
    { :db_name => TEST_DB }
  end

  let(:names) do
    [ 'berlin', 'london' ]
  end

  let(:op) do
    described_class.new(spec)
  end

  describe '#execute' do

    before do
      names.each do |name|
        authorized_client[name].insert_one(x: 1)
      end
    end

    after do
      names.each do |name|
        authorized_client[name].drop
      end
    end

    let(:info) do
      docs = op.execute(authorized_primary.context).documents
      docs.collect { |info| info['name'].sub("#{TEST_DB}.", '') }
    end

    it 'returns the list of collection info' do
      expect(info).to include(*names)
    end
  end
end

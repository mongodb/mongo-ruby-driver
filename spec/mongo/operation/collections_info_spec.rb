require 'spec_helper'

describe Mongo::Operation::CollectionsInfo do

  let(:spec) do
    { selector: { listCollections: 1 },
      db_name: SpecConfig.instance.test_db
    }
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
      docs = authorized_primary.with_connection do |connection|
        op.execute(connection, client: nil).documents
      end

      docs.collect { |info| info['name'].sub("#{SpecConfig.instance.test_db}.", '') }
    end

    it 'returns the list of collection info' do
      expect(info).to include(*names)
    end
  end
end

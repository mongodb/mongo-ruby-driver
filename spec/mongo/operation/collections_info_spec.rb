# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe Mongo::Operation::CollectionsInfo do
  require_no_required_api_version

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

  let(:context) { Mongo::Operation::Context.new }

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
      docs = op.execute(authorized_primary, context: context).documents
      docs.collect { |info| info['name'].sub("#{SpecConfig.instance.test_db}.", '') }
    end

    it 'returns the list of collection info' do
      expect(info).to include(*names)
    end
  end
end

require 'spec_helper'

describe Mongo::Bulk::BulkWrite do

  context 'ordered' do

    let(:bulk) do
      described_class.new(authorized_collection, ordered: true)
    end

    #it_behaves_like 'a bulk write object'
  end

  context 'unordered' do

    let(:bulk) do
      described_class.new(authorized_collection, ordered: false)
    end

    #it_behaves_like 'a bulk write object'
  end
end

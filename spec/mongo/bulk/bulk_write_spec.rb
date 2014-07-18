require 'spec_helper'

describe Mongo::Bulk::BulkWrite do

  let(:write_concern) { Mongo::WriteConcern::Mode.get(:w => 1) }
  let(:database) { Mongo::Database.new(double('client'), :test) }
  let(:collection) do
    Mongo::Collection.new(database, 'users').tap do |c|
      allow(c).to receive(:write_concern) { write_concern }
    end
  end

  context 'ordered' do
    let(:bulk) { described_class.new(collection, :ordered => true) }

    it_behaves_like 'a bulk write object'
  end

  context 'unordered' do
    let(:bulk) { described_class.new(collection, :ordered => false) }

    it_behaves_like 'a bulk write object'
  end
end

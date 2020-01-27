require 'spec_helper'

describe 'Compression' do
  require_compression

  let(:client) do
    new_local_client(SpecConfig.instance.addresses,
      SpecConfig.instance.all_test_options.merge(compressors: %w(zlib)))
  end

  let(:collection) { client['compression'] }

  before do
    collection.delete_many
    collection.insert_one(_id: 1)
  end

  it 'is used' do
    Mongo::Protocol::Compressed.should receive(:new).at_least(:once).and_call_original
    collection.find.to_a.should == [{'_id' => 1}]
  end
end

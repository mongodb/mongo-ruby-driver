require 'spec_helper'

describe 'BSON & command size limits' do
  it 'raises an exception when document size is 16MiB' do
    max_size = 16*1024*1024

    document = { key: 'a' * (max_size - 15) }
    expect(document.to_bson.length).to eq(max_size)

    authorized_collection.insert_one(document)
  end
end

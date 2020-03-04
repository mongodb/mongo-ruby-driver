require 'spec_helper'

describe 'BSON object size' do
  it 'raises an exception when document size is 16MiB' do
    client = new_local_client(SpecConfig.instance.addresses)
    max_size = 16777216 # 16 MiB

    document = { key: 'a' * (max_size - 15) }
    expect(document.to_bson.length).to eq(max_size)

    # This raises an error
    client.use(:db)[:coll].insert_one(document)
  end
end

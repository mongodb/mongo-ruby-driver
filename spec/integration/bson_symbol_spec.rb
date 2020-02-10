require 'spec_helper'

describe 'Symbol encoding to BSON' do
  let(:value) { :foo }

  let(:hash) do
    {'foo' => value}
  end

  let(:serialized) do
    hash.to_bson.to_s
  end

  let(:expected) do
    "\x12\x00\x00\x00\x0Efoo\x00\x04\x00\x00\x00foo\x00\x00".force_encoding('binary')
  end

  it 'encodes symbol to BSON symbol' do
    serialized.should == expected
  end

  it 'round-trips symbol values' do
    Hash.from_bson(hash.to_bson).should == hash
  end
end

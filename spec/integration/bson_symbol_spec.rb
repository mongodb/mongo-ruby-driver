# frozen_string_literal: true
# encoding: utf-8

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
    (+"\x12\x00\x00\x00\x0Efoo\x00\x04\x00\x00\x00foo\x00\x00").force_encoding('binary')
  end

  it 'encodes symbol to BSON symbol' do
    expect(serialized).to eq(expected)
  end

  it 'round-trips symbol values' do
    buffer = BSON::ByteBuffer.new(serialized)
    expect(Hash.from_bson(buffer)).to eq(hash)
  end

  it 'round-trips symbol values using the same byte buffer' do
    if BSON::Environment.jruby? && (BSON::VERSION.split('.').map(&:to_i) <=> [4, 11, 0]) < 0
      skip 'This test is only relevant to bson versions that increment ByteBuffer '\
       'read and write positions separately in JRuby, as implemented in ' \
       'bson version 4.11.0. For more information, see https://jira.mongodb.org/browse/RUBY-2128'
    end

    expect(Hash.from_bson(hash.to_bson)).to eq(hash)
  end
end

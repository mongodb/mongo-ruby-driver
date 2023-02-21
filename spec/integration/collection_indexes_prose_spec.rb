# frozen_string_literal: true
# encoding: utf-8

require 'spec_helper'

describe 'Mongo::Collection#indexes / listIndexes prose tests' do
  let(:collection) do
    authorized_client['list-indexes-prose']
  end

  before do
    collection.drop
    collection.create
    collection.indexes.create_one({name: 1}, name: 'simple')
    collection.indexes.create_one({hello: 1, world: -1}, name: 'compound')
    collection.indexes.create_one({test: 1}, unique: true, name: 'unique')
    collection.insert_one(
      name: 'Stanley',
      hello: 'Yes',
      world: 'No',
      test: 'Always',
    )
  end

  let(:index_list) do
    collection.indexes.to_a
  end

  it 'returns all index names' do
    %w(simple compound unique).each do |name|
      expect(index_list.detect do |spec|
        spec['name'] = name
      end).to be_a(Hash)
    end
  end

  it 'does not return duplicate or nonexistent index names' do
    # There are 4 total indexes: 3 that we explicitly defined + the
    # implicit index on _id.
    expect(index_list.length).to eq(4)
  end

  it 'returns the unique flag for unique index' do
    unique_index = index_list.detect do |spec|
      spec['name'] == 'unique'
    end
    expect(unique_index['unique']).to be true
  end

  it 'does not return the unique flag for non-unique index' do
    %w(simple compound).each do |name|
      index = index_list.detect do |spec|
        spec['name'] == name
      end
      expect(index['unique']).to be nil
    end
  end
end

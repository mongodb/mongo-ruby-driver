# frozen_string_literal: true

require 'spec_helper'

class SearchIndexHelper
  attr_reader :client, :collection_name

  def initialize(client)
    @client = client

    # https://github.com/mongodb/specifications/blob/master/source/index-management/tests/README.rst#id4
    # "...each test uses a randomly generated collection name.  Drivers may
    # generate this collection name however they like, but a suggested
    # implementation is a hex representation of an ObjectId..."
    @collection_name = BSON::ObjectId.new.to_s
  end

  # `soft_create` means to create the collection object without forcing it to
  # be created in the database.
  def collection(soft_create: false)
    @collection ||= client.database[collection_name].tap do |collection|
      collection.create unless soft_create
    end
  end

  # Wait for all of the indexes with the given names to be ready; then return
  # the list of index definitions corresponding to those names.
  def wait_for(*names, &condition)
    timeboxed_wait do
      result = collection.search_indexes
      return filter_results(result, names) if names.all? { |name| ready?(result, name, &condition) }
    end
  end

  # Wait until all of the indexes with the given names are absent from the
  # search index list.
  def wait_for_absense_of(*names)
    names.each do |name|
      timeboxed_wait do
        break if collection.search_indexes(name: name).empty?
      end
    end
  end

  private

  def timeboxed_wait(step: 5, max: 300)
    start = Mongo::Utils.monotonic_time

    loop do
      yield

      sleep step
      raise Timeout::Error, 'wait took too long' if Mongo::Utils.monotonic_time - start > max
    end
  end

  # Returns true if the list of search indexes includes one with the given name,
  # which is ready to be queried.
  def ready?(list, name, &condition)
    condition ||= ->(index) { index['queryable'] }
    list.any? { |index| index['name'] == name && condition[index] }
  end

  def filter_results(result, names)
    result.select { |index| names.include?(index['name']) }
  end
end

describe 'Mongo::Collection#search_indexes prose tests' do
  # https://github.com/mongodb/specifications/blob/master/source/index-management/tests/README.rst#id5
  # "These tests must run against an Atlas cluster with a 7.0+ server."
  require_atlas

  let(:client) do
    Mongo::Client.new(
      ENV['ATLAS_URI'],
      database: SpecConfig.instance.test_db,
      ssl: true,
      ssl_verify: true
    )
  end

  let(:helper) { SearchIndexHelper.new(client) }

  let(:name) { 'test-search-index' }
  let(:definition) { { 'mappings' => { 'dynamic' => false } } }
  let(:create_index) { helper.collection.search_indexes.create_one(definition, name: name) }

  # Case 1: Driver can successfully create and list search indexes
  context 'when creating and listing search indexes' do
    let(:index) { helper.wait_for(name).first }

    it 'succeeds' do
      expect(create_index).to be == name
      expect(index['latestDefinition']).to be == definition
    end
  end

  # Case 2: Driver can successfully create multiple indexes in batch
  context 'when creating multiple indexes in batch' do
    let(:specs) do
      [
        { 'name' => 'test-search-index-1', 'definition' => definition },
        { 'name' => 'test-search-index-2', 'definition' => definition }
      ]
    end

    let(:names) { specs.map { |spec| spec['name'] } }
    let(:create_indexes) { helper.collection.search_indexes.create_many(specs) }

    let(:indexes) { helper.wait_for(*names) }

    let(:index1) { indexes[0] }
    let(:index2) { indexes[1] }

    it 'succeeds' do
      expect(create_indexes).to be == names
      expect(index1['latestDefinition']).to be == specs[0]['definition']
      expect(index2['latestDefinition']).to be == specs[1]['definition']
    end
  end

  # Case 3: Driver can successfully drop search indexes
  context 'when dropping search indexes' do
    it 'succeeds' do
      expect(create_index).to be == name
      helper.wait_for(name)

      helper.collection.search_indexes.drop_one(name: name)

      expect { helper.wait_for_absense_of(name) }.not_to raise_error
    end
  end

  # Case 4: Driver can update a search index
  context 'when updating search indexes' do
    let(:new_definition) { { 'mappings' => { 'dynamic' => true } } }

    let(:index) do
      helper
        .wait_for(name) { |idx| idx['queryable'] && idx['status'] == 'READY' }
        .first
    end

    # rubocop:disable RSpec/ExampleLength
    it 'succeeds' do
      expect(create_index).to be == name
      helper.wait_for(name)

      expect do
        helper.collection.search_indexes.update_one(new_definition, name: name)
      end.not_to raise_error

      expect(index['latestDefinition']).to be == new_definition
    end
    # rubocop:enable RSpec/ExampleLength
  end

  # Case 5: dropSearchIndex suppresses namespace not found errors
  context 'when dropping a non-existent search index' do
    it 'ignores `namespace not found` errors' do
      collection = helper.collection(soft_create: true)
      expect { collection.search_indexes.drop_one(name: name) }
        .not_to raise_error
    end
  end
end

# frozen_string_literal: true

require 'spec_helper'

describe 'Find operation options' do
  let(:subscriber) { Mrss::EventSubscriber.new }

  let(:seeds) do
    [ SpecConfig.instance.addresses.first ]
  end

  let(:client) do
    ClientRegistry.instance.new_local_client(
      seeds,
      SpecConfig.instance.test_options.merge(client_options)
    ).tap do |client|
      client.subscribe(Mongo::Monitoring::COMMAND, subscriber)
    end
  end

  let(:collection) do
    client['find_options', collection_options]
  end

  let(:find_command) do
    subscriber.started_events.find { |cmd| cmd.command_name == 'find' }
  end

  before do
    ClientRegistry.instance.global_client('authorized')['find_options'].drop
    collection.insert_many([ { a: 1 }, { a: 2 }, { a: 3 } ])
  end

  describe 'collation' do
    let(:client_options) do
      {}
    end

    let(:collation) do
      { 'locale' => 'en_US' }
    end

    context 'when defined on the collection' do
      let(:collection_options) do
        { collation: collation }
      end

      it 'uses the collation defined on the collection' do
        collection.find.to_a
        expect(find_command.command['collation']).to be_nil
      end
    end

    context 'when defined on the operation' do
      let(:collection_options) do
        {}
      end

      it 'uses the collation defined on the collection' do
        collection.find({}, collation: collation).to_a
        expect(find_command.command['collation']).to eq(collation)
      end
    end

    context 'when defined on both collection and operation' do
      let(:collection_options) do
        { 'locale' => 'de_AT' }
      end

      it 'uses the collation defined on the collection' do
        collection.find({}, collation: collation).to_a
        expect(find_command.command['collation']).to eq(collation)
      end
    end
  end
end

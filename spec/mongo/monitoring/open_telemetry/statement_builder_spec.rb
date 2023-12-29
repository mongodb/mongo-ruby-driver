# frozen_string_literal: true

require 'spec_helper'

RSpec.describe Mongo::Monitoring::OpenTelemetry::StatementBuilder do
  shared_examples 'statement builder tests' do |obfuscate|
    subject(:statement_builder) { described_class.new(command, obfuscate) }

    context 'with a find command' do
      let(:command) { { 'find' => 'users', 'filter' => { 'name' => 'John' }, '$db' => 'test', 'lsid' => 'some_id' } }

      it 'ignores specified keys' do
        statement = JSON.parse(statement_builder.build)
        Mongo::Protocol::Msg::INTERNAL_KEYS.each do |key|
          expect(statement).not_to have_key(key)
        end
      end

      it 'returns the correct statement' do
        expected_filter = obfuscate ? { 'name' => '?' } : { 'name' => 'John' }
        expected_statement = { 'find' => 'users', 'filter' => expected_filter }.to_json.freeze
        expect(statement_builder.build).to eq(expected_statement)
      end
    end

    context 'with an aggregation pipeline' do
      let(:pipeline) do
        [ { '$match' => { 'status' => 'A' } },
          { '$group' => { '_id' => '$cust_id', 'total' => { '$sum' => '$amount' } } } ]
      end
      let(:command) { { 'aggregate' => 'orders', 'pipeline' => pipeline, '$db' => 'test' } }

      let(:expected_pipeline) do
        if obfuscate
          [ { '$match' => { 'status' => '?' } },
            { '$group' => { '_id' => '?', 'total' => { '$sum' => '?' } } } ]
        else
          pipeline
        end
      end

      it 'returns the correct statement' do
        expected_statement = { 'aggregate' => 'orders', 'pipeline' => expected_pipeline }.to_json.freeze
        expect(statement_builder.build).to eq(expected_statement)
      end
    end

    context 'with a findAndModify command' do
      let(:command) do
        { 'findAndModify' => 'users', 'query' => { 'name' => 'John' }, 'update' => { '$set' => { 'age' => 30 } },
          '$db' => 'test' }
      end

      it 'returns the correct statement' do
        expected_query = obfuscate ? { 'name' => '?' } : { 'name' => 'John' }
        expected_update = obfuscate ? { '$set' => { 'age' => '?' } } : { '$set' => { 'age' => 30 } }
        expected_statement = { 'findAndModify' => 'users', 'query' => expected_query,
                               'update' => expected_update }.to_json.freeze
        expect(statement_builder.build).to eq(expected_statement)
      end
    end

    context 'with an update command' do
      let(:command) do
        { 'update' => 'users', 'updates' => [ { 'q' => { 'name' => 'John' }, 'u' => { '$set' => { 'age' => 30 } } } ],
          '$db' => 'test' }
      end

      it 'returns the correct statement' do
        expected_updates = if obfuscate
                             [ { 'q' => { 'name' => '?' },
                                 'u' => { '$set' => { 'age' => '?' } } } ]
                           else
                             [ { 'q' => { 'name' => 'John' },
                                 'u' => { '$set' => { 'age' => 30 } } } ]
                           end
        expected_statement = { 'update' => 'users', 'updates' => expected_updates }.to_json.freeze
        expect(statement_builder.build).to eq(expected_statement)
      end
    end

    context 'with a delete command' do
      let(:command) do
        { 'delete' => 'users', 'deletes' => [ { 'q' => { 'name' => 'John' }, 'limit' => 1 } ], '$db' => 'test' }
      end

      let(:expected_deletes) do
        if obfuscate
          [ { 'q' => { 'name' => '?' },
              'limit' => '?' } ]
        else
          [ { 'q' => { 'name' => 'John' }, 'limit' => 1 } ]
        end
      end

      it 'returns the correct statement' do
        expected_statement = { 'delete' => 'users', 'deletes' => expected_deletes }.to_json.freeze
        expect(statement_builder.build).to eq(expected_statement)
      end
    end
  end

  context 'when obfuscation is false' do
    include_examples 'statement builder tests', false
  end

  context 'when obfuscation is true' do
    include_examples 'statement builder tests', true
  end
end

# frozen_string_literal: true
# rubocop:todo all

# TODO convert, move or delete these tests as part of RUBY-2706.

=begin
require 'spec_helper'

describe Mongo::Collection::View::Builder::OpQuery do

  describe '#specification' do

    let(:filter) do
      { 'name' => 'test' }
    end

    let(:builder) do
      described_class.new(view)
    end

    let(:specification) do
      builder.specification
    end

    let(:view) do
      Mongo::Collection::View.new(authorized_collection, filter, options)
    end

    context 'when there are modifiers in the options' do

      let(:options) do
        {
          sort: { _id: 1 },
          projection: { name: 1 },
          hint: { name: 1 },
          skip: 10,
          limit: 20,
          batch_size: 5,
          single_batch: false,
          comment: "testing",
          max_scan: 200,
          max_time_ms: 40,
          max_value: { name: 'joe' },
          min_value: { name: 'albert' },
          return_key: true,
          show_disk_loc: true,
          snapshot: true,
          tailable: true,
          oplog_replay: true,
          no_cursor_timeout: true,
          tailable_await: true,
          allow_partial_results: true,
          read_concern: { level: 'local' }
        }
      end

      let(:selector) do
        specification[:selector]
      end

      let(:opts) do
        specification[:options]
      end

      let(:flags) do
        opts[:flags]
      end

      it 'maps the collection name' do
        expect(specification[:coll_name]).to eq(authorized_collection.name)
      end

      it 'maps the filter' do
        expect(selector['$query']).to eq(filter)
      end

      it 'maps sort' do
        expect(selector['$orderby']).to eq('_id' => 1)
      end

      it 'maps projection' do
        expect(opts['project']).to eq('name' => 1)
      end

      it 'maps hint' do
        expect(selector['$hint']).to eq('name' => 1)
      end

      it 'maps skip' do
        expect(opts['skip']).to eq(10)
      end

      it 'maps limit' do
        expect(opts['limit']).to eq(20)
      end

      it 'maps batch size' do
        expect(opts['batch_size']).to eq(5)
      end

      it 'maps comment' do
        expect(selector['$comment']).to eq('testing')
      end

      it 'maps max scan' do
        expect(selector['$maxScan']).to eq(200)
      end

      it 'maps max time ms' do
        expect(selector['$maxTimeMS']).to eq(40)
      end

      it 'maps max' do
        expect(selector['$max']).to eq('name' => 'joe')
      end

      it 'maps min' do
        expect(selector['$min']).to eq('name' => 'albert')
      end

      it 'does not map read concern' do
        expect(selector['$readConcern']).to be_nil
        expect(selector['readConcern']).to be_nil
        expect(opts['readConcern']).to be_nil
      end

      it 'maps return key' do
        expect(selector['$returnKey']).to be true
      end

      it 'maps show record id' do
        expect(selector['$showDiskLoc']).to be true
      end

      it 'maps snapshot' do
        expect(selector['$snapshot']).to be true
      end

      it 'maps tailable' do
        expect(flags).to include(:tailable_cursor)
      end

      it 'maps oplog replay' do
        expect(flags).to include(:oplog_replay)
      end

      it 'maps no cursor timeout' do
        expect(flags).to include(:no_cursor_timeout)
      end

      it 'maps await data' do
        expect(flags).to include(:await_data)
      end

      it 'maps allow partial results' do
        expect(flags).to include(:partial)
      end
    end
  end
end
=end

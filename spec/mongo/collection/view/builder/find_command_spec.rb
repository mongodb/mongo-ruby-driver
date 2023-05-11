# frozen_string_literal: true
# rubocop:todo all

# TODO convert, move or delete these tests as part of RUBY-2706.

=begin
require 'lite_spec_helper'

describe Mongo::Collection::View::Builder::FindCommand do

  let(:client) do
    new_local_client_nmio(['127.0.0.1:27017'])
  end

  let(:base_collection) { client['find-command-spec'] }

  describe '#specification' do

    let(:view) do
      Mongo::Collection::View.new(base_collection, filter, options)
    end

    let(:builder) do
      described_class.new(view, nil)
    end

    let(:specification) do
      builder.specification
    end

    let(:selector) do
      specification[:selector]
    end

    context 'when the options are standard' do

      let(:filter) do
        { 'name' => 'test' }
      end

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
          await_data: true,
          allow_partial_results: true,
          collation: { locale: 'en_US' }
        }
      end

      context 'when the operation has a session' do

        let(:session) do
          double('session')
        end

        let(:builder) do
          described_class.new(view, session)
        end

        it 'adds the session to the specification' do
          expect(builder.specification[:session]).to be(session)
        end
      end

      it 'maps the collection name' do
        expect(selector['find']).to eq(base_collection.name)
      end

      it 'maps the filter' do
        expect(selector['filter']).to eq(filter)
      end

      it 'maps sort' do
        expect(selector['sort']).to eq('_id' => 1)
      end

      it 'maps projection' do
        expect(selector['projection']).to eq('name' => 1)
      end

      it 'maps hint' do
        expect(selector['hint']).to eq('name' => 1)
      end

      it 'maps skip' do
        expect(selector['skip']).to eq(10)
      end

      it 'maps limit' do
        expect(selector['limit']).to eq(20)
      end

      it 'maps batch size' do
        expect(selector['batchSize']).to eq(5)
      end

      it 'maps single batch' do
        expect(selector['singleBatch']).to be false
      end

      it 'maps comment' do
        expect(selector['comment']).to eq('testing')
      end

      it 'maps max scan' do
        expect(selector['maxScan']).to eq(200)
      end

      it 'maps max time ms' do
        expect(selector['maxTimeMS']).to eq(40)
      end

      it 'maps max' do
        expect(selector['max']).to eq('name' => 'joe')
      end

      it 'maps min' do
        expect(selector['min']).to eq('name' => 'albert')
      end

      it 'maps return key' do
        expect(selector['returnKey']).to be true
      end

      it 'maps show record id' do
        expect(selector['showRecordId']).to be true
      end

      it 'maps snapshot' do
        expect(selector['snapshot']).to be true
      end

      it 'maps tailable' do
        expect(selector['tailable']).to be true
      end

      it 'maps oplog_replay' do
        expect(selector['oplogReplay']).to be true
      end

      it 'warns when using oplog_replay' do
        client.should receive(:log_warn).with('oplogReplay is deprecated and ignored by MongoDB 4.4 and later')
        selector
      end

      it 'maps no cursor timeout' do
        expect(selector['noCursorTimeout']).to be true
      end

      it 'maps await data' do
        expect(selector['awaitData']).to be true
      end

      it 'maps allow partial results' do
        expect(selector['allowPartialResults']).to be true
      end

      it 'maps collation' do
        expect(selector['collation']).to eq('locale' => 'en_US')
      end
    end


    context 'when there is a limit' do

      let(:filter) do
        { 'name' => 'test' }
      end

      context 'when limit is 0' do

        context 'when batch_size is also 0' do

          let(:options) do
            { limit: 0, batch_size: 0 }
          end

          it 'does not set the singleBatch' do
            expect(selector['singleBatch']).to be nil
          end

          it 'does not set the limit' do
            expect(selector['limit']).to be nil
          end

          it 'does not set the batch size' do
            expect(selector['batchSize']).to be nil
          end
        end

        context 'when batch_size is not set' do

          let(:options) do
            { limit: 0 }
          end

          it 'does not set the singleBatch' do
            expect(selector['singleBatch']).to be nil
          end

          it 'does not set the limit' do
            expect(selector['limit']).to be nil
          end

          it 'does not set the batch size' do
            expect(selector['batchSize']).to be nil
          end
        end
      end

      context 'when the limit is negative' do

        context 'when there is a batch_size' do

          context 'when the batch_size is positive' do

            let(:options) do
              { limit: -1, batch_size: 3 }
            end

            it 'sets single batch to true' do
              expect(selector['singleBatch']).to be true
            end

            it 'converts the limit to a positive value' do
              expect(selector['limit']).to be(options[:limit].abs)
            end

            it 'sets the batch size' do
              expect(selector['batchSize']).to be(options[:batch_size])
            end
          end

          context 'when the batch_size is negative' do

            let(:options) do
              { limit: -1, batch_size: -3 }
            end

            it 'sets single batch to true' do
              expect(selector['singleBatch']).to be true
            end

            it 'converts the limit to a positive value' do
              expect(selector['limit']).to be(options[:limit].abs)
            end

            it 'sets the batch size to the limit' do
              expect(selector['batchSize']).to be(options[:limit].abs)
            end
          end
        end

        context 'when there is not a batch_size' do

          let(:options) do
            { limit: -5 }
          end

          it 'sets single batch to true' do
            expect(selector['singleBatch']).to be true
          end

          it 'converts the limit to a positive value' do
            expect(selector['limit']).to be(options[:limit].abs)
          end

          it 'does not set the batch size' do
            expect(selector['batchSize']).to be_nil
          end
        end
      end

      context 'when the limit is positive' do

        context 'when there is a batch_size' do

          context 'when the batch_size is positive' do

            let(:options) do
              { limit: 5, batch_size: 3 }
            end

            it 'does not set singleBatch' do
              expect(selector['singleBatch']).to be nil
            end

            it 'sets the limit' do
              expect(selector['limit']).to be(options[:limit])
            end

            it 'sets the batch size' do
              expect(selector['batchSize']).to be(options[:batch_size])
            end
          end

          context 'when the batch_size is negative' do

            let(:options) do
              { limit: 5, batch_size: -3 }
            end

            it 'sets the singleBatch' do
              expect(selector['singleBatch']).to be true
            end

            it 'sets the limit' do
              expect(selector['limit']).to be(options[:limit])
            end

            it 'sets the batch size to a positive value' do
              expect(selector['batchSize']).to be(options[:batch_size].abs)
            end
          end
        end

        context 'when there is not a batch_size' do

          let(:options) do
            { limit: 5 }
          end

          it 'does not set the singleBatch' do
            expect(selector['singleBatch']).to be nil
          end

          it 'sets the limit' do
            expect(selector['limit']).to be(options[:limit])
          end

          it 'does not set the batch size' do
            expect(selector['batchSize']).to be nil
          end
        end
      end
    end

    context 'when there is a batch_size' do

      let(:filter) do
        { 'name' => 'test' }
      end

      context 'when there is no limit' do

        context 'when the batch_size is positive' do

          let(:options) do
            { batch_size: 3 }
          end

          it 'does not set the singleBatch' do
            expect(selector['singleBatch']).to be nil
          end

          it 'does not set the limit' do
            expect(selector['limit']).to be nil
          end

          it 'sets the batch size' do
            expect(selector['batchSize']).to be(options[:batch_size])
          end
        end

        context 'when the batch_size is negative' do

          let(:options) do
            { batch_size: -3 }
          end

          it 'sets the singleBatch' do
            expect(selector['singleBatch']).to be true
          end

          it 'does not set the limit' do
            expect(selector['limit']).to be nil
          end

          it 'sets the batch size to a positive value' do
            expect(selector['batchSize']).to be(options[:batch_size].abs)
          end
        end

        context 'when batch_size is 0' do

          let(:options) do
            { batch_size: 0 }
          end

          it 'does not set the singleBatch' do
            expect(selector['singleBatch']).to be nil
          end

          it 'does not set the limit' do
            expect(selector['limit']).to be nil
          end

          it 'does not set the batch size' do
            expect(selector['batchSize']).to be nil
          end
        end
      end
    end

    context 'when limit and batch_size are negative' do

      let(:filter) do
        { 'name' => 'test' }
      end

      let(:options) do
        { limit: -1, batch_size: -3 }
      end

      it 'sets single batch to true' do
        expect(selector['singleBatch']).to be true
      end

      it 'converts the limit to a positive value' do
        expect(selector['limit']).to be(options[:limit].abs)
      end
    end

    context 'when cursor_type is specified' do

      let(:filter) do
        { 'name' => 'test' }
      end

      context 'when cursor_type is :tailable' do

        let(:options) do
          {
            cursor_type: :tailable,
          }
        end

        it 'maps to tailable' do
          expect(selector['tailable']).to be true
        end

        it 'does not map to awaitData' do
          expect(selector['awaitData']).to be_nil
        end
      end

      context 'when cursor_type is :tailable_await' do

        let(:options) do
          {
            cursor_type: :tailable_await,
          }
        end

        it 'maps to tailable' do
          expect(selector['tailable']).to be true
        end

        it 'maps to awaitData' do
          expect(selector['awaitData']).to be true
        end
      end
    end

    context 'when the collection has a read concern defined' do

      let(:collection) do
        base_collection.with(read_concern: { level: 'invalid' })
      end

      let(:view) do
        Mongo::Collection::View.new(collection, {})
      end

      it 'applies the read concern of the collection' do
        expect(selector['readConcern']).to eq(BSON::Document.new(level: 'invalid'))
      end

      context 'when explain is called for the find' do

        let(:collection) do
          base_collection.with(read_concern: { level: 'invalid' })
        end

        let(:view) do
          Mongo::Collection::View.new(collection, {})
        end

        it 'applies the read concern of the collection' do
          expect( builder.explain_specification[:selector][:explain][:readConcern]).to eq(BSON::Document.new(level: 'invalid'))
        end
      end
    end

    context 'when the collection does not have a read concern defined' do

      let(:filter) do
        {}
      end

      let(:options) do
        {}
      end

      it 'does not apply a read concern' do
        expect(selector['readConcern']).to be_nil
      end
    end
  end
end
=end

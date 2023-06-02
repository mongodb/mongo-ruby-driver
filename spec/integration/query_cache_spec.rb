# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe 'QueryCache' do
  around do |spec|
    Mongo::QueryCache.clear
    Mongo::QueryCache.cache { spec.run }
  end

  before do
    authorized_collection.delete_many
    subscriber.clear_events!
  end

  before(:all) do
    # It is likely that there are other session leaks in the driver that are
    # unrelated to the query cache. Clear the SessionRegistry at the start of
    # these tests in order to detect leaks that occur only within the scope of
    # these tests.
    #
    # Other session leaks will be detected and addressed as part of RUBY-2391.
    Mrss::SessionRegistry.instance.clear_registry
  end

  after do
    Mrss::SessionRegistry.instance.verify_sessions_ended!
  end

  let(:subscriber) { Mrss::EventSubscriber.new }

  let(:client) do
    authorized_client.tap do |client|
      client.subscribe(Mongo::Monitoring::COMMAND, subscriber)
    end
  end

  let(:authorized_collection) { client['collection_spec'] }

  let(:events) do
    subscriber.command_started_events('find')
  end

  describe '#cache' do

    before do
      Mongo::QueryCache.enabled = false
      authorized_collection.insert_one({ name: 'testing' })
      authorized_collection.find(name: 'testing').to_a
    end

    it 'enables the query cache inside the block' do
      Mongo::QueryCache.cache do
        authorized_collection.find(name: 'testing').to_a
        expect(Mongo::QueryCache.enabled?).to be(true)
        expect(Mongo::QueryCache.send(:cache_table).length).to eq(1)
        expect(events.length).to eq(2)
      end
      authorized_collection.find(name: 'testing').to_a
      expect(Mongo::QueryCache.enabled?).to be(false)
      expect(Mongo::QueryCache.send(:cache_table).length).to eq(1)
      expect(events.length).to eq(2)
    end
  end

  describe '#uncached' do

    before do
      authorized_collection.insert_one({ name: 'testing' })
      authorized_collection.find(name: 'testing').to_a
    end

    it 'disables the query cache inside the block' do
      expect(Mongo::QueryCache.send(:cache_table).length).to eq(1)
      Mongo::QueryCache.uncached do
        authorized_collection.find(name: 'testing').to_a
        expect(Mongo::QueryCache.enabled?).to be(false)
        expect(events.length).to eq(2)
      end
      authorized_collection.find(name: 'testing').to_a
      expect(Mongo::QueryCache.enabled?).to be(true)
      expect(Mongo::QueryCache.send(:cache_table).length).to eq(1)
      expect(events.length).to eq(2)
    end
  end

  describe 'query with multiple batches' do

    before do
      102.times { |i| authorized_collection.insert_one(_id: i) }
    end

    let(:expected_results) { [*0..101].map { |id| { "_id" => id } } }

    it 'returns the correct result' do
      result = authorized_collection.find.to_a
      expect(result.length).to eq(102)
      expect(result).to eq(expected_results)
    end

    it 'returns the correct result multiple times' do
      result1 = authorized_collection.find.to_a
      result2 = authorized_collection.find.to_a
      expect(result1).to eq(expected_results)
      expect(result2).to eq(expected_results)
    end

    it 'caches the query' do
      authorized_collection.find.to_a
      authorized_collection.find.to_a
      expect(subscriber.command_started_events('find').length).to eq(1)
      expect(subscriber.command_started_events('getMore').length).to eq(1)
    end

    it 'uses cached cursor when limited' do
      authorized_collection.find.to_a
      result = authorized_collection.find({}, limit: 5).to_a

      expect(result.length).to eq(5)
      expect(result).to eq(expected_results.first(5))

      expect(subscriber.command_started_events('find').length).to eq(1)
      expect(subscriber.command_started_events('getMore').length).to eq(1)
    end

    it 'can be used with a block API' do
      authorized_collection.find.to_a

      result = []
      authorized_collection.find.each do |doc|
        result << doc
      end

      expect(result).to eq(expected_results)

      expect(subscriber.command_started_events('find').length).to eq(1)
      expect(subscriber.command_started_events('getMore').length).to eq(1)
    end

    context 'when the cursor isn\'t fully iterated the first time' do
      it 'continues iterating' do
        result1 = authorized_collection.find.first(5)

        expect(result1.length).to eq(5)
        expect(result1).to eq(expected_results.first(5))

        expect(subscriber.command_started_events('find').length).to eq(1)
        expect(subscriber.command_started_events('getMore').length).to eq(0)

        result2 = authorized_collection.find.to_a

        expect(result2.length).to eq(102)
        expect(result2).to eq(expected_results)

        expect(subscriber.command_started_events('find').length).to eq(1)
        expect(subscriber.command_started_events('getMore').length).to eq(1)
      end

      it 'can be iterated multiple times' do
        authorized_collection.find.first(5)
        authorized_collection.find.to_a

        result = authorized_collection.find.to_a

        expect(result.length).to eq(102)
        expect(result).to eq(expected_results)

        expect(subscriber.command_started_events('find').length).to eq(1)
        expect(subscriber.command_started_events('getMore').length).to eq(1)
      end

      it 'can be used with a block API' do
        authorized_collection.find.first(5)

        result = []
        authorized_collection.find.each do |doc|
          result << doc
        end

        expect(result.length).to eq(102)
        expect(result).to eq(expected_results)

        expect(subscriber.command_started_events('find').length).to eq(1)
        expect(subscriber.command_started_events('getMore').length).to eq(1)
      end
    end
  end

  describe 'queries with read concern' do
    require_wired_tiger
    min_server_fcv '3.6'

    before do
      authorized_client['test', write_concern: { w: :majority }].drop
    end

    context 'when two queries have same read concern' do
      before do
        authorized_client['test', read_concern: { level: :majority }].find.to_a
        authorized_client['test', read_concern: { level: :majority }].find.to_a
      end

      it 'executes one query' do
        expect(events.length).to eq(1)
      end
    end

    context 'when two queries have different read concerns' do
      before do
        authorized_client['test', read_concern: { level: :majority }].find.to_a
        authorized_client['test', read_concern: { level: :local }].find.to_a
      end

      it 'executes two queries' do
        expect(events.length).to eq(2)
      end
    end
  end

  describe 'queries with read preference' do
    before do
      subscriber.clear_events!
      authorized_client['test'].drop
    end

    context 'when two queries have different read preferences' do
      before do
        authorized_client['test', read: { mode: :primary }].find.to_a
        authorized_client['test', read: { mode: :primary_preferred }].find.to_a
      end

      it 'executes two queries' do
        expect(events.length).to eq(2)
      end
    end

    context 'when two queries have same read preference' do
      before do
        authorized_client['test', read: { mode: :primary }].find.to_a
        authorized_client['test', read: { mode: :primary }].find.to_a
      end

      it 'executes one query' do
        expect(events.length).to eq(1)
      end
    end
  end

  describe 'query fills up entire batch' do
    before do
      subscriber.clear_events!
      authorized_client['test'].drop

      2.times { |i| authorized_client['test'].insert_one(_id: i) }
    end

    let(:expected_result) do
      [{ "_id" => 0 }, { "_id" => 1 }]
    end

    # When the last batch runs out, try_next will return nil instead of a
    # document. This test checks that nil is not added to the list of cached
    # documents or returned as a result.
    it 'returns the correct response' do
      expect(authorized_client['test'].find({}, batch_size: 2).to_a).to eq(expected_result)
      expect(authorized_client['test'].find({}, batch_size: 2).to_a).to eq(expected_result)
    end
  end

  context 'when querying in the same collection' do

    before do
      10.times do |i|
        authorized_collection.insert_one(test: i)
      end
    end

    context 'when query cache is disabled' do

      before do
        Mongo::QueryCache.enabled = false
        authorized_collection.find(test: 1).to_a
      end

      it 'queries again' do
        authorized_collection.find(test: 1).to_a
        expect(events.length).to eq(2)
        expect(Mongo::QueryCache.send(:cache_table).length).to eq(0)
      end
    end

    context 'when query cache is enabled' do

      before do
        authorized_collection.find(test: 1).to_a
      end

      it 'does not query again' do
        authorized_collection.find(test: 1).to_a
        expect(events.length).to eq(1)
        expect(Mongo::QueryCache.send(:cache_table).length).to eq(1)
      end
    end

    context 'when query has collation' do
      min_server_fcv '3.4'

      let(:options1) do
        { :collation => { locale: 'fr' } }
      end

      let(:options2) do
        { collation: { locale: 'en_US' } }
      end

      before do
        authorized_collection.find({ test: 3 }, options1).to_a
      end

      context 'when query has the same collation' do

        it 'uses the cache' do
          authorized_collection.find({ test: 3 }, options1).to_a
          expect(events.length).to eq(1)
        end
      end

      context 'when query has a different collation' do

        it 'queries again' do
          authorized_collection.find({ test: 3 }, options2).to_a
          expect(events.length).to eq(2)
          expect(Mongo::QueryCache.send(:cache_table)['ruby-driver.collection_spec'].length).to eq(2)
        end
      end
    end

    describe 'queries with limits' do
      context 'when the first query has no limit and the second does' do
        before do
          authorized_collection.find.to_a.count
        end

        it 'uses the cache' do
          results_limit_5 = authorized_collection.find.limit(5).to_a
          results_limit_negative_5 = authorized_collection.find.limit(-5).to_a
          results_limit_3 = authorized_collection.find.limit(3).to_a
          results_limit_negative_3 = authorized_collection.find.limit(-3).to_a
          results_no_limit = authorized_collection.find.to_a
          results_limit_0 = authorized_collection.find.limit(0).to_a


          expect(results_limit_5.length).to eq(5)
          expect(results_limit_5.map { |r| r["test"] }).to eq([0, 1, 2, 3, 4])

          expect(results_limit_negative_5.length).to eq(5)
          expect(results_limit_negative_5.map { |r| r["test"] }).to eq([0, 1, 2, 3, 4])

          expect(results_limit_3.length).to eq(3)
          expect(results_limit_3.map { |r| r["test"] }).to eq([0, 1, 2])

          expect(results_limit_negative_3.length).to eq(3)
          expect(results_limit_negative_3.map { |r| r["test"] }).to eq([0, 1, 2])

          expect(results_no_limit.length).to eq(10)
          expect(results_no_limit.map { |r| r["test"] }).to eq([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])

          expect(results_limit_0.length).to eq(10)
          expect(results_limit_0.map { |r| r["test"] }).to eq([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])

          expect(events.length).to eq(1)
        end
      end

      context 'when the first query has a 0 limit' do
        before do
          authorized_collection.find.limit(0).to_a
        end

        it 'uses the cache' do
          results_limit_5 = authorized_collection.find.limit(5).to_a
          results_limit_negative_5 = authorized_collection.find.limit(-5).to_a
          results_limit_3 = authorized_collection.find.limit(3).to_a
          results_limit_negative_3 = authorized_collection.find.limit(-3).to_a
          results_no_limit = authorized_collection.find.to_a
          results_limit_0 = authorized_collection.find.limit(0).to_a

          expect(results_limit_5.length).to eq(5)
          expect(results_limit_5.map { |r| r["test"] }).to eq([0, 1, 2, 3, 4])

          expect(results_limit_negative_5.length).to eq(5)
          expect(results_limit_negative_5.map { |r| r["test"] }).to eq([0, 1, 2, 3, 4])


          expect(results_limit_3.length).to eq(3)
          expect(results_limit_3.map { |r| r["test"] }).to eq([0, 1, 2])

          expect(results_limit_negative_3.length).to eq(3)
          expect(results_limit_negative_3.map { |r| r["test"] }).to eq([0, 1, 2])


          expect(results_no_limit.length).to eq(10)
          expect(results_no_limit.map { |r| r["test"] }).to eq([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])


          expect(results_limit_0.length).to eq(10)
          expect(results_limit_0.map { |r| r["test"] }).to eq([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])

          expect(events.length).to eq(1)
        end
      end

      context 'when the first query has a limit' do
        before do
          authorized_collection.find.limit(2).to_a
        end

        context 'and the second query has a larger limit' do
          let(:results) { authorized_collection.find.limit(3).to_a }

          it 'queries again' do
            expect(results.length).to eq(3)
            expect(results.map { |result| result["test"] }).to eq([0, 1, 2])
            expect(events.length).to eq(2)
          end
        end

        context 'and two queries are performed with a larger limit' do
          it 'uses the query cache for the third query' do
            results1 = authorized_collection.find.limit(3).to_a
            results2 = authorized_collection.find.limit(3).to_a

            expect(results1.length).to eq(3)
            expect(results1.map { |r| r["test"] }).to eq([0, 1, 2])

            expect(results2.length).to eq(3)
            expect(results2.map { |r| r["test"] }).to eq([0, 1, 2])

            expect(events.length).to eq(2)
          end
        end

        context 'and two queries are performed with a larger negative limit' do
          it 'uses the query cache for the third query' do
            results1 = authorized_collection.find.limit(-3).to_a
            results2 = authorized_collection.find.limit(-3).to_a

            expect(results1.length).to eq(3)
            expect(results1.map { |r| r["test"] }).to eq([0, 1, 2])

            expect(results2.length).to eq(3)
            expect(results2.map { |r| r["test"] }).to eq([0, 1, 2])

            expect(events.length).to eq(2)
          end
        end

        context 'and the second query has a smaller limit' do
          let(:results) { authorized_collection.find.limit(1).to_a }

          it 'uses the cached query' do
            expect(results.count).to eq(1)
            expect(results.first["test"]).to eq(0)
            expect(events.length).to eq(1)
          end
        end

        context 'and the second query has a smaller negative limit' do
          let(:results) { authorized_collection.find.limit(-1).to_a }

          it 'uses the cached query' do
            expect(results.count).to eq(1)
            expect(results.first["test"]).to eq(0)
            expect(events.length).to eq(1)
          end
        end

        context 'and the second query has no limit' do
          it 'queries again' do
            expect(authorized_collection.find.to_a.count).to eq(10)
            expect(events.length).to eq(2)
          end
        end
      end

      context 'when the first query has a negative limit' do
        before do
          authorized_collection.find.limit(-2).to_a
        end

        context 'and the second query has a larger limit' do
          let(:results) { authorized_collection.find.limit(3).to_a }

          it 'queries again' do
            expect(results.length).to eq(3)
            expect(results.map { |result| result["test"] }).to eq([0, 1, 2])
            expect(events.length).to eq(2)
          end
        end

        context 'and the second query has a larger negative limit' do
          let(:results) { authorized_collection.find.limit(-3).to_a }

          it 'queries again' do
            expect(results.length).to eq(3)
            expect(results.map { |result| result["test"] }).to eq([0, 1, 2])
            expect(events.length).to eq(2)
          end
        end

        context 'and two queries are performed with a larger limit' do
          it 'uses the query cache for the third query' do
            results1 = authorized_collection.find.limit(3).to_a
            results2 = authorized_collection.find.limit(3).to_a

            expect(results1.length).to eq(3)
            expect(results1.map { |r| r["test"] }).to eq([0, 1, 2])

            expect(results2.length).to eq(3)
            expect(results2.map { |r| r["test"] }).to eq([0, 1, 2])

            expect(events.length).to eq(2)
          end
        end

        context 'and two queries are performed with a larger negative limit' do
          it 'uses the query cache for the third query' do
            results1 = authorized_collection.find.limit(-3).to_a
            results2 = authorized_collection.find.limit(-3).to_a

            expect(results1.length).to eq(3)
            expect(results1.map { |r| r["test"] }).to eq([0, 1, 2])

            expect(results2.length).to eq(3)
            expect(results2.map { |r| r["test"] }).to eq([0, 1, 2])

            expect(events.length).to eq(2)
          end
        end

        context 'and the second query has a smaller limit' do
          let(:results) { authorized_collection.find.limit(1).to_a }

          it 'uses the cached query' do
            expect(results.count).to eq(1)
            expect(results.first["test"]).to eq(0)
            expect(events.length).to eq(1)
          end
        end

        context 'and the second query has a smaller negative limit' do
          let(:results) { authorized_collection.find.limit(-1).to_a }

          it 'uses the cached query' do
            expect(results.count).to eq(1)
            expect(results.first["test"]).to eq(0)
            expect(events.length).to eq(1)
          end
        end

        context 'and the second query has no limit' do
          it 'queries again' do
            expect(authorized_collection.find.to_a.count).to eq(10)
            expect(events.length).to eq(2)
          end
        end
      end
    end

    context 'when querying only the first' do

      before do
        5.times do |i|
          authorized_collection.insert_one(test: 11)
        end
      end

      before do
        authorized_collection.find({test: 11}).to_a
      end

      it 'does not query again' do
        expect(authorized_collection.find({test: 11}).count).to eq(5)
        authorized_collection.find({test: 11}).first
        expect(events.length).to eq(1)
      end

      context 'when limiting the result' do

        it 'does not query again' do
          authorized_collection.find({test: 11}, limit: 2).to_a
          expect(authorized_collection.find({test: 11}, limit: 2).to_a.count).to eq(2)
          expect(events.length).to eq(1)
        end
      end
    end

    context 'when specifying a different skip value' do

      before do
        authorized_collection.find({}, {limit: 2, skip: 3}).to_a
      end

      it 'queries again' do
        results = authorized_collection.find({}, {limit: 2, skip: 5}).to_a
        expect(results.count).to eq(2)
        expect(events.length).to eq(2)
      end
    end

    context 'when sorting documents' do

      before do
        authorized_collection.find({}, desc).to_a
      end

      let(:desc) do
        { sort: {test: -1} }
      end

      let(:asc) do
        { sort: {test: 1} }
      end

      context 'with different selector' do

        it 'queries again' do
          authorized_collection.find({}, asc).to_a
          expect(events.length).to eq(2)
        end
      end

      it 'does not query again' do
        authorized_collection.find({}, desc).to_a
        expect(events.length).to eq(1)
      end
    end

    context 'when inserting new documents' do
      context 'when inserting and querying from same collection' do
        before do
          authorized_collection.find.to_a
          authorized_collection.insert_one({ name: "bob" })
        end

        it 'queries again' do
          authorized_collection.find.to_a
          expect(events.length).to eq(2)
        end
      end

      context 'when inserting and querying from different collections' do
        before do
          authorized_collection.find.to_a
          authorized_client['different_collection'].insert_one({ name: "bob" })
        end

        it 'uses the cached query' do
          authorized_collection.find.to_a
          expect(events.length).to eq(1)
        end
      end
    end

    [:delete_many, :delete_one].each do |method|
      context "when deleting with #{method}" do
        context 'when deleting and querying from same collection' do
          before do
            authorized_collection.find.to_a
            authorized_collection.send(method)
          end

          it 'queries again' do
            authorized_collection.find.to_a
            expect(events.length).to eq(2)
          end
        end

        context 'when deleting and querying from different collections' do
          before do
            authorized_collection.find.to_a
            authorized_client['different_collection'].send(method)
          end

          it 'uses the cached query' do
            authorized_collection.find.to_a
            expect(events.length).to eq(1)
          end
        end
      end
    end

    [:find_one_and_delete, :find_one_and_replace, :find_one_and_update,
      :replace_one].each do |method|
      context "when updating with #{method}" do
        context 'when updating and querying from same collection' do
          before do
            authorized_collection.find.to_a
            authorized_collection.send(method, { field: 'value' }, { field: 'new value' })
          end

          it 'queries again' do
            authorized_collection.find.to_a
            expect(events.length).to eq(2)
          end
        end

        context 'when updating and querying from different collections' do
          before do
            authorized_collection.find.to_a
            authorized_client['different_collection'].send(method, { field: 'value' }, { field: 'new value' })
          end

          it 'uses the cached query' do
            authorized_collection.find.to_a
            expect(events.length).to eq(1)
          end
        end
      end
    end

    [:update_one, :update_many].each do |method|
      context "when updating with ##{method}" do
        context 'when updating and querying from same collection' do
          before do
            authorized_collection.find.to_a
            authorized_collection.send(method, { field: 'value' }, { "$inc" => { :field =>  1 } })
          end

          it 'queries again' do
            authorized_collection.find.to_a
            expect(events.length).to eq(2)
          end
        end

        context 'when updating and querying from different collections' do
          before do
            authorized_collection.find.to_a
            authorized_client['different_collection'].send(method, { field: 'value' }, { "$inc" => { :field =>  1 } })
          end

          it 'uses the cached query' do
            authorized_collection.find.to_a
            expect(events.length).to eq(1)
          end
        end
      end
    end

    context 'when performing bulk write' do
      context 'with insert_one' do
        context 'when inserting and querying from same collection' do
          before do
            authorized_collection.find.to_a
            authorized_collection.bulk_write([ { insert_one: { name: 'bob' } } ])
          end

          it 'queries again' do
            authorized_collection.find.to_a
            expect(events.length).to eq(2)
          end
        end

        context 'when inserting and querying from different collection' do
          before do
            authorized_collection.find.to_a
            authorized_client['different_collection'].bulk_write(
              [ { insert_one: { name: 'bob' } } ]
            )
          end

          it 'uses the cached query' do
            authorized_collection.find.to_a
            expect(events.length).to eq(1)
          end
        end
      end

      [:update_one, :update_many].each do |method|
        context "with #{method}" do
          context 'when updating and querying from same collection' do
            before do
              authorized_collection.find.to_a
              authorized_collection.bulk_write([
                {
                  method => {
                    filter: { field: 'value' },
                    update: { '$set' => { field: 'new value' } }
                  }
                }
              ])
            end

            it 'queries again' do
              authorized_collection.find.to_a
              expect(events.length).to eq(2)
            end
          end

          context 'when updating and querying from different collection' do
            before do
              authorized_collection.find.to_a
              authorized_client['different_collection'].bulk_write([
                {
                  method => {
                    filter: { field: 'value' },
                    update: { '$set' => { field: 'new value' } }
                  }
                }
              ])
            end

            it 'uses the cached query' do
              authorized_collection.find.to_a
              expect(events.length).to eq(1)
            end
          end
        end
      end

      [:delete_one, :delete_many].each do |method|
        context "with #{method}" do
          context 'when delete and querying from same collection' do
            before do
              authorized_collection.find.to_a
              authorized_collection.bulk_write([
                {
                  method => {
                    filter: { field: 'value' },
                  }
                }
              ])
            end

            it 'queries again' do
              authorized_collection.find.to_a
              expect(events.length).to eq(2)
            end
          end

          context 'when delete and querying from different collection' do
            before do
              authorized_collection.find.to_a
              authorized_client['different_collection'].bulk_write([
                {
                  method => {
                    filter: { field: 'value' },
                  }
                }
              ])
            end

            it 'uses the cached query' do
              authorized_collection.find.to_a
              expect(events.length).to eq(1)
            end
          end
        end
      end

      context 'with replace_one' do
        context 'when replacing and querying from same collection' do
          before do
            authorized_collection.find.to_a
            authorized_collection.bulk_write([
              {
                replace_one: {
                  filter: { field: 'value' },
                  replacement: { field: 'new value' }
                }
              }
            ])
          end

          it 'queries again' do
            authorized_collection.find.to_a
            expect(events.length).to eq(2)
          end
        end

        context 'when replacing and querying from different collection' do
          before do
            authorized_collection.find.to_a
            authorized_client['different_collection'].bulk_write([
              {
                replace_one: {
                  filter: { field: 'value' },
                  replacement: { field: 'new value' }
                }
              }
            ])
          end

          it 'uses the cached query' do
            authorized_collection.find.to_a
            expect(events.length).to eq(1)
          end
        end
      end

      context 'when query occurs between bulk write creation and execution' do
        before do
          authorized_collection.delete_many
        end

        it 'queries again' do
          bulk_write = Mongo::BulkWrite.new(
            authorized_collection,
            [{ insert_one: { test: 1 } }]
          )

          expect(authorized_collection.find(test: 1).to_a.length).to eq(0)
          bulk_write.execute
          expect(authorized_collection.find(test: 1).to_a.length).to eq(1)
          expect(events.length).to eq(2)
        end
      end
    end

    context 'when aggregating with $out' do
      before do
        authorized_collection.find.to_a
        authorized_collection.aggregate([
          { '$match' => { test: 1 } },
          { '$out' => { coll: 'new_coll' } }
        ])
      end

      it 'queries again' do
        authorized_collection.find.to_a
        expect(events.length).to eq(2)
      end

      it 'clears the cache' do
        expect(Mongo::QueryCache.send(:cache_table)).to be_empty
      end
    end

    context 'when aggregating with $merge' do
      min_server_fcv '4.2'

      before do
        authorized_collection.delete_many
        authorized_collection.find.to_a
        authorized_collection.aggregate([
          { '$match' => { 'test' => 1 } },
          { '$merge' => {
              into: {
                db: SpecConfig.instance.test_db,
                coll: 'new_coll',
              },
              on: "_id",
              whenMatched: "replace",
              whenNotMatched: "insert",
            }
          }
        ])
      end

      it 'queries again' do
        authorized_collection.find.to_a
        expect(events.length).to eq(2)
      end

      it 'clears the cache' do
        expect(Mongo::QueryCache.send(:cache_table)).to be_empty
      end
    end
  end

  context 'when aggregating' do
    before do
      3.times { authorized_collection.insert_one(test: 1) }
    end

    let(:events) do
      subscriber.command_started_events('aggregate')
    end

    let(:aggregation) do
      authorized_collection.aggregate([ { '$match' => { test: 1 } } ])
    end

    it 'caches the aggregation' do
      expect(aggregation.to_a.length).to eq(3)
      expect(aggregation.to_a.length).to eq(3)
      expect(events.length).to eq(1)
    end

    context 'with read concern' do
      require_wired_tiger
      min_server_fcv '3.6'

      let(:aggregation_read_concern) do
        authorized_client['collection_spec', { read_concern: { level: :local } }]
          .aggregate([ { '$match' => { test: 1 } } ])
      end

      it 'queries twice' do
        expect(aggregation.to_a.length).to eq(3)
        expect(aggregation_read_concern.to_a.length).to eq(3)
        expect(events.length).to eq(2)
      end
    end

    context 'with read preference' do
      let(:aggregation_read_preference) do
        authorized_client['collection_spec', { read: { mode: :primary } }]
          .aggregate([ { '$match' => { test: 1 } } ])
      end

      it 'queries twice' do
        expect(aggregation.to_a.length).to eq(3)
        expect(aggregation_read_preference.to_a.length).to eq(3)
        expect(events.length).to eq(2)
      end
    end

    context 'when collation is specified' do
      min_server_fcv '3.4'

      let(:aggregation_collation) do
        authorized_collection.aggregate(
          [ { '$match' => { test: 1 } } ],
          { collation: { locale: 'fr' } }
        )
      end

      it 'queries twice' do
        expect(aggregation.to_a.length).to eq(3)
        expect(aggregation_collation.to_a.length).to eq(3)
        expect(events.length).to eq(2)
      end
    end

    context 'when insert_one is performed on another collection' do
      before do
        aggregation.to_a
        authorized_client['different_collection'].insert_one(name: 'bob')
        aggregation.to_a
      end

      it 'queries again' do
        expect(events.length).to eq(2)
      end
    end

    context 'when insert_many is performed on another collection' do
      before do
        aggregation.to_a
        authorized_client['different_collection'].insert_many([name: 'bob'])
        aggregation.to_a
      end

      it 'queries again' do
        expect(events.length).to eq(2)
      end
    end

    [:delete_many, :delete_one].each do |method|
      context "when #{method} is performed on another collection" do
        before do
          aggregation.to_a
          authorized_client['different_collection'].send(method)
          aggregation.to_a
        end

        it 'queries again' do
          expect(events.length).to eq(2)
        end
      end
    end

    [:find_one_and_delete, :find_one_and_replace, :find_one_and_update,
      :replace_one].each do |method|
      context "when #{method} is performed on another collection" do
        before do
          aggregation.to_a
          authorized_client['different_collection'].send(method, { field: 'value' }, { field: 'new value' })
          aggregation.to_a
        end

        it 'queries again' do
          expect(events.length).to eq(2)
        end
      end
    end

    [:update_one, :update_many].each do |method|
      context 'when update_many is performed on another collection' do
        before do
          aggregation.to_a
          authorized_client['different_collection'].send(method, { field: 'value' }, { "$inc" => { :field =>  1 } })
          aggregation.to_a
        end

        it 'queries again' do
          expect(events.length).to eq(2)
        end
      end
    end

    context '#count_documents' do
      context 'on same collection' do
        it 'caches the query' do
          expect(authorized_collection.count_documents(test: 1)).to eq(3)
          expect(authorized_collection.count_documents(test: 1)).to eq(3)

          expect(events.length).to eq(1)
        end
      end

      context 'on different collections' do
        let(:other_collection) { authorized_client['other_collection'] }

        before do
          other_collection.drop
          6.times { other_collection.insert_one(test: 1) }
        end

        it 'caches the query' do
          expect(authorized_collection.count_documents(test: 1)).to eq(3)
          expect(other_collection.count_documents(test: 1)).to eq(6)

          expect(events.length).to eq(2)
        end
      end
    end
  end

  context 'when find command fails and retries' do
    require_fail_command
    require_no_multi_mongos
    require_warning_clean

    before do
      5.times do |i|
        authorized_collection.insert_one(test: i)
      end
    end

    before do
      client.use('admin').command(
        configureFailPoint: 'failCommand',
        mode: { times: 1 },
        data: {
          failCommands: ['find'],
          closeConnection: true
        }
      )
    end

    let(:command_name) { 'find' }

    it 'uses modern retryable reads when using query cache' do
      expect(Mongo::QueryCache.enabled?).to be(true)

      expect(Mongo::Logger.logger).to receive(:warn).once.with(/modern.*attempt 1/).and_call_original
      authorized_collection.find(test: 1).to_a
      expect(Mongo::QueryCache.send(:cache_table).length).to eq(1)
      expect(subscriber.command_started_events('find').length).to eq(2)

      authorized_collection.find(test: 1).to_a
      expect(Mongo::QueryCache.send(:cache_table).length).to eq(1)
      expect(subscriber.command_started_events('find').length).to eq(2)
    end
  end

  context 'when querying in a different collection' do

    let(:database) { client.database }

    let(:new_collection) do
      Mongo::Collection.new(database, 'foo')
    end

    before do
      authorized_collection.find.to_a
    end

    it 'queries again' do
      new_collection.find.to_a
      expect(Mongo::QueryCache.send(:cache_table).length).to eq(2)
      expect(events.length).to eq(2)
    end
  end

  context 'with system collection' do
    let(:client) do
      ClientRegistry.instance.global_client('root_authorized').tap do |client|
        client.subscribe(Mongo::Monitoring::COMMAND, subscriber)
      end
    end

    before do
      begin
        client.database.users.remove('alanturing')
      rescue Mongo::Error::OperationFailure
        # can be user not found, ignore
      end
    end

    it 'does not use the query cache' do
      client['system.users'].find.to_a
      client['system.users'].find.to_a
      expect(events.length).to eq(2)
    end
  end

  context 'when result set has multiple documents and cursor is iterated partially' do

    before do
      Mongo::QueryCache.enabled = false
      5.times do
        authorized_collection.insert_one({ name: 'testing' })
      end
    end

    shared_examples 'retrieves full result set on second iteration' do
      it 'retrieves full result set on second iteration' do
        Mongo::QueryCache.clear
        Mongo::QueryCache.enabled = true

        partial_first_iteration

        authorized_collection.find.to_a.length.should == 5
      end

    end

    context 'using each & break' do
      let(:partial_first_iteration) do
        called = false
        authorized_collection.find.each do
          called = true
          break
        end
        called.should be true
      end

      include_examples 'retrieves full result set on second iteration'
    end

    context 'using next' do
      let(:partial_first_iteration) do
        # #next is executed in its own fiber, and query cache is disabled
        # for that operation.
        authorized_collection.find.to_enum.next
      end

      include_examples 'retrieves full result set on second iteration'
    end
  end

  describe 'concurrent queries with multiple batches' do

    before do
      102.times { |i| authorized_collection.insert_one(_id: i) }
    end

    # The query cache table is stored in thread local storage, so even though
    # we executed the same queries in the first thread (and waited for them to
    # finish), that query is going to be executed again (only once) in the
    # second thread.
    it "uses separate cache tables per thread" do
      thread1 = Thread.new do
        Mongo::QueryCache.cache do
          authorized_collection.find.to_a
          authorized_collection.find.to_a
          authorized_collection.find.to_a
          authorized_collection.find.to_a
        end
      end
      thread1.join
      thread2 = Thread.new do
        Mongo::QueryCache.cache do
          authorized_collection.find.to_a
          authorized_collection.find.to_a
          authorized_collection.find.to_a
          authorized_collection.find.to_a
        end
      end
      thread2.join

      expect(subscriber.command_started_events('find').length).to eq(2)
      expect(subscriber.command_started_events('getMore').length).to eq(2)
    end

    it "is able to query concurrently" do
      wait_for_first_thread = true
      wait_for_second_thread = true
      threads = []
      first_thread_docs = []
      threads << Thread.new do
        Mongo::QueryCache.cache do
          # 1. iterate first batch
          authorized_collection.find.each_with_index do |doc, i|
            # 2. verify that we're getting all of the correct documents
            first_thread_docs << doc
            expect(doc).to eq({ "_id" => i })
            if i == 50
              # 2. check that there hasn't been a getmore
              expect(subscriber.command_started_events('getMore').length).to eq(0)
              # 3. mark second thread ready to start
              wait_for_first_thread = false
              # 4. wait for second thread
              sleep 0.1 while wait_for_second_thread
              # 5. verify that the other thread sent a getmore
              expect(subscriber.command_started_events('getMore').length).to eq(1)
            end
            # 6. finish iterating the batch
          end
          # 7. verify that it still caches the query
          authorized_collection.find.to_a
        end
      end

      threads << Thread.new do
        Mongo::QueryCache.cache do
          # 1. wait for the first thread to finish first batch iteration
          sleep 0.1 while wait_for_first_thread
          # 2. iterate the entire result set
          authorized_collection.find.each_with_index do |doc, i|
            # 3. verify documnents
            expect(doc).to eq({ "_id" => i })
          end
          # 4. verify get more
          expect(subscriber.command_started_events('getMore').length).to eq(1)
          # 5. mark second thread done
          wait_for_second_thread = false
          # 6. verify that it still caches the query
          authorized_collection.find.to_a
        end
      end

      threads.map(&:join)
      expect(first_thread_docs.length).to eq(102)
      expect(subscriber.command_started_events('find').length).to eq(2)
      expect(subscriber.command_started_events('getMore').length).to eq(2)
    end
  end
end

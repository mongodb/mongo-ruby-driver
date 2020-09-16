require 'spec_helper'

describe 'QueryCache' do
  around do |spec|
    Mongo::QueryCache.clear_cache
    Mongo::QueryCache.cache { spec.run }
  end

  before do
    authorized_collection.delete_many
    subscriber.clear_events!
  end

  let(:subscriber) { EventSubscriber.new }

  let(:client) do
    authorized_client.tap do |client|
      client.subscribe(Mongo::Monitoring::COMMAND, subscriber)
    end
  end

  let(:authorized_collection) { client['collection_spec'] }

  describe '#cache' do

    before do
      Mongo::QueryCache.enabled = false
      authorized_collection.insert_one({ name: 'testing' })
      authorized_collection.find(name: 'testing').to_a
    end

    let(:events) do
      subscriber.command_started_events('find')
    end

    it 'enables the query cache inside the block' do
      Mongo::QueryCache.cache do
        authorized_collection.find(name: 'testing').to_a
        expect(Mongo::QueryCache.enabled?).to be(true)
        expect(Mongo::QueryCache.cache_table.length).to eq(1)
        expect(events.length).to eq(2)
      end
      authorized_collection.find(name: 'testing').to_a
      expect(Mongo::QueryCache.enabled?).to be(false)
      expect(Mongo::QueryCache.cache_table.length).to eq(1)
      expect(events.length).to eq(2)
    end
  end

  describe '#uncached' do

    before do
      authorized_collection.insert_one({ name: 'testing' })
      authorized_collection.find(name: 'testing').to_a
    end

    let(:events) do
      subscriber.command_started_events('find')
    end

    it 'disables the query cache inside the block' do
      expect(Mongo::QueryCache.cache_table.length).to eq(1)
      Mongo::QueryCache.uncached do
        authorized_collection.find(name: 'testing').to_a
        expect(Mongo::QueryCache.enabled?).to be(false)
        expect(events.length).to eq(2)
      end
      authorized_collection.find(name: 'testing').to_a
      expect(Mongo::QueryCache.enabled?).to be(true)
      expect(Mongo::QueryCache.cache_table.length).to eq(1)
      expect(events.length).to eq(2)
    end
  end

  describe 'iterating cursors multiple times' do
    context 'when query returns single batch' do
      before do
        100.times { |i| authorized_collection.insert_one(_id: i) }
      end

      let(:expected_results) { [*0..99].map { |id| { "_id" => id } } }

      it 'does not raise an exception' do
        result1 = authorized_collection.find.to_a
        expect(result1.length).to eq(100)
        expect(result1).to eq(expected_results)

        result2 = authorized_collection.find.to_a
        expect(result2.length).to eq(100)
        expect(result2).to eq(expected_results)

        # Verify that the driver performs the query once
        expect(subscriber.command_started_events('find').length).to eq(1)
        expect(subscriber.command_started_events('getMore').length).to eq(0)
      end
    end

    context 'when query returns multiple batches' do
      before do
        101.times { |i| authorized_collection.insert_one(_id: i) }
      end

      let(:expected_results) { [*0..100].map { |id| { "_id" => id } } }

      it 'performs the query once and returns the correct results' do
        result1 = authorized_collection.find.to_a
        expect(result1.length).to eq(101)
        expect(result1).to eq(expected_results)

        result2 = authorized_collection.find.to_a
        expect(result2.length).to eq(101)
        expect(result2).to eq(expected_results)

        # Verify that the driver performs the query once
        expect(subscriber.command_started_events('find').length).to eq(1)
        expect(subscriber.command_started_events('getMore').length).to eq(1)
      end
    end
  end

  describe 'queries with read concern' do
    require_wired_tiger
    min_server_fcv '3.6'

    before do
      authorized_client['test'].drop
    end

    let(:events) do
      subscriber.command_started_events('find')
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

    let(:events) do
      subscriber.command_started_events('find')
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

  context 'when querying in the same collection' do

    before do
      10.times do |i|
        authorized_collection.insert_one(test: i)
      end
    end

    let(:events) do
      subscriber.command_started_events('find')
    end

    context 'when query cache is disabled' do

      before do
        Mongo::QueryCache.enabled = false
        authorized_collection.find(test: 1).to_a
      end

      it 'queries again' do
        authorized_collection.find(test: 1).to_a
        expect(events.length).to eq(2)
        expect(Mongo::QueryCache.cache_table.length).to eq(0)
      end
    end

    context 'when query cache is enabled' do

      before do
        authorized_collection.find(test: 1).to_a
      end

      it 'does not query again' do
        authorized_collection.find(test: 1).to_a
        expect(events.length).to eq(1)
        expect(Mongo::QueryCache.cache_table.length).to eq(1)
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
          expect(Mongo::QueryCache.cache_table.length).to eq(2)
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
          results_limit_3 = authorized_collection.find.limit(3).to_a
          results_no_limit = authorized_collection.find.to_a

          expect(results_limit_5.length).to eq(5)
          expect(results_limit_5.map { |r| r["test"] }).to eq([0, 1, 2, 3, 4])

          expect(results_limit_3.length).to eq(3)
          expect(results_limit_3.map { |r| r["test"] }).to eq([0, 1, 2])

          expect(results_no_limit.length).to eq(10)
          expect(results_no_limit.map { |r| r["test"] }).to eq([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])

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
          before do
            if ClusterConfig.instance.fcv_ish <= '3.0'
              pending 'RUBY-2367 Server versions 3.0 and older execute three' \
                'queries in this case. This should be resolved when the query' \
                'cache is modified to cache multi-batch queries.'
            end
          end

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

        context 'and the second query has a smaller limit' do
          before do
            if ClusterConfig.instance.fcv_ish <= '3.0'
              pending 'RUBY-2367 Server versions 3.0 and older execute two' \
                'queries in this case. This should be resolved when the query' \
                'cache is modified to cache multi-batch queries.'
            end
          end

          let(:results) { authorized_collection.find.limit(1).to_a }

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

      before do
        authorized_collection.find.to_a
        authorized_collection.insert_one({ name: "bob" })
      end

      it 'queries again' do
        expect(Mongo::QueryCache.cache_table.length).to eq(0)
        authorized_collection.find.to_a
        expect(events.length).to eq(2)
      end
    end

    context 'when deleting documents' do

      before do
        authorized_collection.find.to_a
        authorized_collection.delete_many
      end

      it 'queries again' do
        expect(Mongo::QueryCache.cache_table.length).to eq(0)
        authorized_collection.find.to_a
        expect(events.length).to eq(2)
      end
    end

    context 'when replacing documents' do
      before do
        authorized_collection.find.to_a
        authorized_collection.replace_one(selector, { test: 100 } )
      end

      let(:selector) do
        { test: 5 }
      end

      it 'queries again' do
        expect(Mongo::QueryCache.cache_table.length).to eq(0)
        authorized_collection.find.to_a
        expect(events.length).to eq(2)
      end
    end
  end

  context 'when find command fails and retries' do
    require_fail_command
    require_no_multi_shard
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
      expect(Mongo::QueryCache.cache_table.length).to eq(1)
      expect(subscriber.command_started_events('find').length).to eq(2)

      authorized_collection.find(test: 1).to_a
      expect(Mongo::QueryCache.cache_table.length).to eq(1)
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

    let(:events) do
      subscriber.command_started_events('find')
    end

    it 'queries again' do
      new_collection.find.to_a
      expect(Mongo::QueryCache.cache_table.length).to eq(2)
      expect(events.length).to eq(2)
    end
  end

  describe 'in transactions' do
    require_transaction_support
    require_wired_tiger

    let(:collection) { authorized_client['test'] }

    let(:events) do
      subscriber.command_started_events('find')
    end

    before do
      Utils.create_collection(authorized_client, 'test')
    end

    context 'with convenient API' do
      context 'when same query is performed inside and outside of transaction' do
        it 'performs one query' do
          collection.find.to_a

          session = authorized_client.start_session
          session.with_transaction do
            collection.find({}, session: session).to_a
          end

          expect(subscriber.command_started_events('find').length).to eq(1)
        end
      end

      context 'when transaction has a different read concern' do
        it 'performs two queries' do
          collection.find.to_a

          session = authorized_client.start_session
          session.with_transaction(
           read_concern: { level: :snapshot }
          ) do
            collection.find({}, session: session).to_a
          end

          expect(subscriber.command_started_events('find').length).to eq(2)
        end
      end

      context 'when transaction has a different read preference' do
        it 'performs two queries' do
          collection.find.to_a

          session = authorized_client.start_session
          session.with_transaction(
           read: { mode: :primary }
          ) do
            collection.find({}, session: session).to_a
          end

          expect(subscriber.command_started_events('find').length).to eq(2)
        end
      end

      context 'when transaction is committed' do
        it 'clears the cache' do
          session = authorized_client.start_session
          session.with_transaction do
            collection.insert_one({ test: 1 }, session: session)
            collection.insert_one({ test: 2 }, session: session)

            expect(collection.find({}, session: session).to_a.length).to eq(2)
            expect(collection.find({}, session: session).to_a.length).to eq(2)

            # The driver caches the queries within the transaction
            expect(subscriber.command_started_events('find').length).to eq(1)
            session.commit_transaction
          end

          expect(collection.find.to_a.length).to eq(2)

          # The driver clears the cache and runs the query again
          expect(subscriber.command_started_events('find').length).to eq(2)
        end
      end

      context 'when transaction is aborted' do
        it 'clears the cache' do
          session = authorized_client.start_session
          session.with_transaction do
            collection.insert_one({ test: 1 }, session: session)
            collection.insert_one({ test: 2 }, session: session)

            expect(collection.find({}, session: session).to_a.length).to eq(2)
            expect(collection.find({}, session: session).to_a.length).to eq(2)

            # The driver caches the queries within the transaction
            expect(subscriber.command_started_events('find').length).to eq(1)
            session.abort_transaction
          end

          expect(collection.find.to_a.length).to eq(0)

          # The driver clears the cache and runs the query again
          expect(subscriber.command_started_events('find').length).to eq(2)
        end
      end
    end

    context 'with low-level API' do
      context 'when transaction is committed' do
        it 'clears the cache' do
          session = authorized_client.start_session
          session.start_transaction

          collection.insert_one({ test: 1 }, session: session)
          collection.insert_one({ test: 2 }, session: session)

          expect(collection.find({}, session: session).to_a.length).to eq(2)
          expect(collection.find({}, session: session).to_a.length).to eq(2)

          # The driver caches the queries within the transaction
          expect(subscriber.command_started_events('find').length).to eq(1)

          session.commit_transaction

          expect(collection.find.to_a.length).to eq(2)

          # The driver clears the cache and runs the query again
          expect(subscriber.command_started_events('find').length).to eq(2)
        end
      end

      context 'when transaction is aborted' do
        it 'clears the cache' do
          session = authorized_client.start_session
          session.start_transaction

          collection.insert_one({ test: 1 }, session: session)
          collection.insert_one({ test: 2 }, session: session)

          expect(collection.find({}, session: session).to_a.length).to eq(2)
          expect(collection.find({}, session: session).to_a.length).to eq(2)

          # The driver caches the queries within the transaction
          expect(subscriber.command_started_events('find').length).to eq(1)

          session.abort_transaction

          expect(collection.find.to_a.length).to eq(0)

          # The driver clears the cache and runs the query again
          expect(subscriber.command_started_events('find').length).to eq(2)
        end
      end
    end
  end
end

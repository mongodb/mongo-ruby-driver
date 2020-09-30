require 'spec_helper'

describe 'QueryCache with transactions' do
  around do |spec|
    Mongo::QueryCache.clear
    Mongo::QueryCache.cache { spec.run }
  end

  before do
    authorized_collection.delete_many
    subscriber.clear_events!
  end

  # These tests do not currently use the session registry because transactions
  # leak sessions independently of the query cache. This will be resolved by
  # RUBY-2391.

  let(:subscriber) { EventSubscriber.new }

  let(:client) do
    authorized_client.tap do |client|
      client.subscribe(Mongo::Monitoring::COMMAND, subscriber)
    end
  end

  let(:authorized_collection) { client['collection_spec'] }

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

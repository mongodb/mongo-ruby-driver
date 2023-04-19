# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe 'QueryCache with transactions' do
  # Work around https://jira.mongodb.org/browse/HELP-10518
  before(:all) do
    client = ClientRegistry.instance.global_client('authorized')
    Utils.create_collection(client, 'test')

    Utils.mongos_each_direct_client do |client|
      client['test'].distinct('foo').to_a
    end
  end

  around do |spec|
    Mongo::QueryCache.clear
    Mongo::QueryCache.cache { spec.run }
  end

  # These tests do not currently use the session registry because transactions
  # leak sessions independently of the query cache. This will be resolved by
  # RUBY-2391.

  let(:subscriber) { Mrss::EventSubscriber.new }

  let(:client) do
    authorized_client.tap do |client|
      client.subscribe(Mongo::Monitoring::COMMAND, subscriber)
    end
  end

  before do
    collection.delete_many

    # Work around https://jira.mongodb.org/browse/HELP-10518
    client.start_session do |session|
      session.with_transaction do
        collection.find({}, session: session).to_a
      end
    end
    subscriber.clear_events!
  end

  describe 'in transactions' do
    require_transaction_support
    require_wired_tiger

    let(:collection) { client['test'] }

    let(:events) do
      subscriber.command_started_events('find')
    end

    context 'with convenient API' do
      context 'when same query is performed inside and outside of transaction' do
        it 'performs one query' do
          collection.find.to_a

          session = client.start_session
          session.with_transaction do
            collection.find({}, session: session).to_a
          end

          expect(subscriber.command_started_events('find').length).to eq(1)
        end
      end

      context 'when transaction has a different read concern' do
        it 'performs two queries' do
          collection.find.to_a

          session = client.start_session
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

          session = client.start_session
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
          session = client.start_session
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
          session = client.start_session
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
          session = client.start_session
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
          session = client.start_session
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

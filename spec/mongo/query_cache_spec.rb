# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe Mongo::QueryCache do

  around do |spec|
    Mongo::QueryCache.clear
    Mongo::QueryCache.cache { spec.run }
  end

  before do
    authorized_collection.delete_many
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

  describe '#enabled' do

    context 'when query cache is disabled' do

      before do
        Mongo::QueryCache.enabled = false
      end

      it 'disables the query cache' do
        expect(Mongo::QueryCache.enabled?).to be(false)
      end
    end

    context 'when query cache is enabled' do

      before do
        Mongo::QueryCache.enabled = true
      end

      it 'enables the query cache' do
        expect(Mongo::QueryCache.enabled?).to be(true)
      end
    end
  end

  describe '#cache' do

    before do
      Mongo::QueryCache.enabled = false
    end

    it 'enables the query cache inside the block' do
      Mongo::QueryCache.cache do
        expect(Mongo::QueryCache.enabled?).to be(true)
      end
      expect(Mongo::QueryCache.enabled?).to be(false)
    end
  end

  describe '#uncached' do

    it 'disables the query cache inside the block' do
      Mongo::QueryCache.uncached do
        expect(Mongo::QueryCache.enabled?).to be(false)
      end
      expect(Mongo::QueryCache.enabled?).to be(true)
    end
  end

  describe '#cache_table' do

    before do
      authorized_collection.insert_one({ name: 'testing' })
      authorized_collection.find(name: 'testing').to_a
    end

    it 'gets the cached query' do
      expect(Mongo::QueryCache.send(:cache_table).length).to eq(1)
      authorized_collection.find(name: 'testing').to_a
      expect(events.length).to eq(1)
    end
  end

  describe '#clear' do

    before do
      authorized_collection.insert_one({ name: 'testing' })
      authorized_collection.find(name: 'testing').to_a
    end

    it 'clears the cache' do
      expect(Mongo::QueryCache.send(:cache_table).length).to eq(1)
      Mongo::QueryCache.clear
      expect(Mongo::QueryCache.send(:cache_table).length).to eq(0)
    end
  end

  describe '#set' do
    let(:caching_cursor) { double("Mongo::CachingCursor") }
    let(:namespace) { 'db.coll' }
    let(:selector) { { field: 'value' } }
    let(:skip) { 5 }
    let(:sort) { { field: 'asc' } }
    let(:limit) { 5 }
    let(:projection) { { field: 1 } }
    let(:collation) { { locale: 'fr_CA' } }
    let(:read_concern) { { level: :majority } }
    let(:read_preference) { { mode: :secondary } }

    let(:options) do
      {
        namespace: namespace,
        selector: selector,
        skip: skip,
        sort: sort,
        limit: limit,
        projection: projection,
        collation: collation,
        read_concern: read_concern,
        read_preference: read_preference,
      }
    end

    it 'stores the cursor at the correct key' do
      Mongo::QueryCache.set(caching_cursor, **options)
      expect(Mongo::QueryCache.send(:cache_table)[namespace][[namespace, selector, skip, sort, projection, collation, read_concern, read_preference]]).to eq(caching_cursor)
    end
  end

  describe '#get' do
    let(:view) { double("Mongo::Collection::View") }
    let(:result) do
      double("Mongo::Operation::Result").tap do |result|
        allow(result).to receive(:is_a?).with(Mongo::Operation::Result).and_return(true)
      end
    end
    let(:server) { double("Mongo::Server") }
    let(:caching_cursor) { Mongo::CachingCursor.new(view, result, server) }

    let(:options) do
      {
        namespace: 'db.coll',
        selector: { field: 'value' },
      }
    end

    before do
      allow(result).to receive(:cursor_id) { 0 }
      allow(result).to receive(:namespace) { 'db.coll' }
      allow(result).to receive(:connection_global_id) { 1 }
      allow(view).to receive(:limit) { nil }
    end

    context 'when there is no entry in the cache' do
      it 'returns nil' do
        expect(Mongo::QueryCache.get(**options)).to be_nil
      end
    end

    context 'when there is an entry in the cache' do
      before do
        Mongo::QueryCache.set(caching_cursor, **caching_cursor_options)
      end

      context 'when that entry has no limit' do
        let(:caching_cursor_options) do
          {
            namespace: 'db.coll',
            selector: { field: 'value' },
          }
        end

        let(:query_options) do
          caching_cursor_options.merge(limit: limit)
        end

        context 'when the query has a limit' do
          let(:limit) { 5 }

          it 'returns the caching cursor' do
            expect(Mongo::QueryCache.get(**query_options)).to eq(caching_cursor)
          end
        end

        context 'when the query has a limit but negative' do
          let(:limit) { -5 }

          it 'returns the caching cursor' do
            expect(Mongo::QueryCache.get(**query_options)).to eq(caching_cursor)
          end
        end

        context 'when the query has no limit' do
          let(:limit) { nil }

          it 'returns the caching cursor' do
            expect(Mongo::QueryCache.get(**query_options)).to eq(caching_cursor)
          end
        end

        context 'when the query has a 0 limit' do
          let(:limit) { 0 }

          it 'returns the caching cursor' do
            expect(Mongo::QueryCache.get(**query_options)).to eq(caching_cursor)
          end
        end
      end

      context 'when that entry has a 0 limit' do
        let(:caching_cursor_options) do
          {
            namespace: 'db.coll',
            selector: { field: 'value' },
            limit: 0,
          }
        end

        let(:query_options) do
          caching_cursor_options.merge(limit: limit)
        end

        before do
          allow(view).to receive(:limit) { 0 }
        end

        context 'when the query has a limit' do
          let(:limit) { 5 }

          it 'returns the caching cursor' do
            expect(Mongo::QueryCache.get(**query_options)).to eq(caching_cursor)
          end
        end

        context 'when the query has a limit but negative' do
          let(:limit) { -5 }

          it 'returns the caching cursor' do
            expect(Mongo::QueryCache.get(**query_options)).to eq(caching_cursor)
          end
        end


        context 'when the query has no limit' do
          let(:limit) { nil }

          it 'returns the caching cursor' do
            expect(Mongo::QueryCache.get(**query_options)).to eq(caching_cursor)
          end
        end

        context 'when the query has a 0 limit' do
          let(:limit) { 0 }

          it 'returns the caching cursor' do
            expect(Mongo::QueryCache.get(**query_options)).to eq(caching_cursor)
          end
        end
      end

      context 'when that entry has a limit' do
        let(:caching_cursor_options) do
          {
            namespace: 'db.coll',
            selector: { field: 'value' },
            limit: 5,
          }
        end

        let(:query_options) do
          caching_cursor_options.merge(limit: limit)
        end

        before do
          allow(view).to receive(:limit) { 5 }
        end

        context 'and the new query has a smaller limit' do
          let(:limit) { 4 }

          it 'returns the caching cursor' do
            expect(Mongo::QueryCache.get(**query_options)).to eq(caching_cursor)
          end
        end

        context 'and the new query has a smaller limit but negative' do
          let(:limit) { -4 }

          it 'returns the caching cursor' do
            expect(Mongo::QueryCache.get(**query_options)).to eq(caching_cursor)
          end
        end

        context 'and the new query has a larger limit' do
          let(:limit) { 6 }

          it 'returns nil' do
            expect(Mongo::QueryCache.get(**query_options)).to be_nil
          end
        end

        context 'and the new query has a larger limit but negative' do
          let(:limit) { -6 }

          it 'returns nil' do
            expect(Mongo::QueryCache.get(**query_options)).to be_nil
          end
        end

        context 'and the new query has the same limit' do
          let(:limit) { 5 }

          it 'returns the caching cursor' do
            expect(Mongo::QueryCache.get(**query_options)).to eq(caching_cursor)
          end
        end

        context 'and the new query has the same limit but negative' do
          let(:limit) { -5 }

          it 'returns the caching cursor' do
            expect(Mongo::QueryCache.get(**query_options)).to eq(caching_cursor)
          end
        end

        context 'and the new query has no limit' do
          let(:limit) { nil }

          it 'returns nil' do
            expect(Mongo::QueryCache.get(**query_options)).to be_nil
          end
        end

        context 'and the new query has a 0 limit' do
          let(:limit) { 0 }

          it 'returns nil' do
            expect(Mongo::QueryCache.get(**query_options)).to be_nil
          end
        end
      end

      context 'when that entry has a negative limit' do
        let(:caching_cursor_options) do
          {
            namespace: 'db.coll',
            selector: { field: 'value' },
            limit: -5,
          }
        end

        let(:query_options) do
          caching_cursor_options.merge(limit: limit)
        end

        before do
          allow(view).to receive(:limit) { -5 }
        end

        context 'and the new query has a smaller limit' do
          let(:limit) { 4 }

          it 'returns the caching cursor' do
            expect(Mongo::QueryCache.get(**query_options)).to eq(caching_cursor)
          end
        end

        context 'and the new query has a larger limit' do
          let(:limit) { 6 }

          it 'returns nil' do
            expect(Mongo::QueryCache.get(**query_options)).to be_nil
          end
        end

        context 'and the new query has the same negative limit' do
          let(:limit) { -5 }

          it 'returns the caching cursor' do
            expect(Mongo::QueryCache.get(**query_options)).to eq(caching_cursor)
          end
        end

        context 'and the new query has the same positive limit' do
          let(:limit) { 5 }

          it 'returns the caching cursor' do
            expect(Mongo::QueryCache.get(**query_options)).to eq(caching_cursor)
          end
        end

        context 'and the new query has no limit' do
          let(:limit) { nil }

          it 'returns nil' do
            expect(Mongo::QueryCache.get(**query_options)).to be_nil
          end
        end

        context 'and the new query has a 0 limit' do
          let(:limit) { 0 }

          it 'returns nil' do
            expect(Mongo::QueryCache.get(**query_options)).to be_nil
          end
        end
      end
    end
  end

  describe '#clear_namespace' do
    let(:caching_cursor) { double("Mongo::CachingCursor") }
    let(:namespace1) { 'db.coll' }
    let(:namespace2) { 'db.coll2' }
    let(:namespace3) { 'db.coll3' }
    let(:selector) { { field: 'value' } }

    before do
      Mongo::QueryCache.set(caching_cursor, namespace: namespace1, selector: selector)
      Mongo::QueryCache.set(caching_cursor, namespace: namespace2, selector: selector)
      Mongo::QueryCache.set(caching_cursor, namespace: namespace3, selector: selector, multi_collection: true)
    end

    it 'returns nil' do
      expect(Mongo::QueryCache.clear_namespace(namespace1)).to be_nil
    end

    it 'clears the specified namespace in the query cache' do
      Mongo::QueryCache.clear_namespace(namespace1)
      expect(Mongo::QueryCache.send(:cache_table)[namespace1]).to be_nil
    end

    it 'does not clear other namespaces in the query cache' do
      Mongo::QueryCache.clear_namespace(namespace1)
      expect(Mongo::QueryCache.send(:cache_table)[namespace2]).not_to be_nil
    end

    it 'clears the nil namespace' do
      Mongo::QueryCache.clear_namespace(namespace1)
      expect(Mongo::QueryCache.send(:cache_table)[nil]).to be_nil
    end
  end
end

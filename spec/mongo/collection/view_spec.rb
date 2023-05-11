# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe Mongo::Collection::View do

  let(:filter) do
    {}
  end

  let(:options) do
    {}
  end

  let(:view) do
    described_class.new(authorized_collection, filter, options)
  end

  before do
    authorized_collection.delete_many
  end

  describe '#==' do

    context 'when the other object is not a collection view' do

      let(:other) { 'test' }

      it 'returns false' do
        expect(view).to_not eq(other)
      end
    end

    context 'when the views have the same collection, filter, and options' do

      let(:other) do
        described_class.new(authorized_collection, filter, options)
      end

      it 'returns true' do
        expect(view).to eq(other)
      end
    end

    context 'when two views have a different collection' do

      let(:other_collection) do
        authorized_client[:other]
      end

      let(:other) do
        described_class.new(other_collection, filter, options)
      end

      it 'returns false' do
        expect(view).not_to eq(other)
      end
    end

    context 'when two views have a different filter' do

      let(:other_filter) do
        { 'name' => 'Emily' }
      end

      let(:other) do
        described_class.new(authorized_collection, other_filter, options)
      end

      it 'returns false' do
        expect(view).not_to eq(other)
      end
    end

    context 'when two views have different options' do

      let(:other_options) do
        { 'limit' => 20 }
      end

      let(:other) do
        described_class.new(authorized_collection, filter, other_options)
      end

      it 'returns false' do
        expect(view).not_to eq(other)
      end
    end
  end

  describe 'copy' do

    let(:view_clone) do
      view.clone
    end

    it 'dups the options' do
      expect(view.options).not_to be(view_clone.options)
    end

    it 'dups the filter' do
      expect(view.filter).not_to be(view_clone.filter)
    end

    it 'references the same collection' do
      expect(view.collection).to be(view_clone.collection)
    end
  end

  describe '#each' do

    let(:documents) do
      (1..10).map{ |i| { field: "test#{i}" }}
    end

    before do
      authorized_collection.delete_many
      authorized_collection.insert_many(documents)
    end

    context 'when a block is not provided' do

      let(:enumerator) do
        view.each
      end

      it 'returns an enumerator' do
        enumerator.each do |doc|
          expect(doc).to have_key('field')
        end
      end
    end

    describe '#close_query' do

      let(:options) do
        { :batch_size => 1 }
      end

      let(:cursor) do
        view.instance_variable_get(:@cursor)
      end

      before do
        view.to_enum.next
        if ClusterConfig.instance.fcv_ish < '3.2'
          cursor.instance_variable_set(:@cursor_id, 1)
        end
      end

      it 'sends a kill cursors command for the cursor' do
        expect(cursor).to receive(:close).and_call_original
        view.close_query
      end
    end

    describe 'collation' do

      context 'when the view has a collation set' do

        let(:options) do
          { collation: { locale: 'en_US', strength: 2 } }
        end

        let(:filter) do
          { name: 'BANG' }
        end

        before do
          authorized_collection.insert_one(name: 'bang')
        end

        let(:result) do
          view.limit(-1).first
        end

        context 'when the server selected supports collations' do
          min_server_fcv '3.4'

          it 'applies the collation' do
            expect(result['name']).to eq('bang')
          end
        end

        context 'when the server selected does not support collations' do
          max_server_version '3.2'

          it 'raises an exception' do
            expect {
              result
            }.to raise_exception(Mongo::Error::UnsupportedCollation)
          end

          context 'when a String key is used' do

            let(:options) do
              { 'collation' => { locale: 'en_US', strength: 2 } }
            end

            it 'raises an exception' do
              expect {
                result
              }.to raise_exception(Mongo::Error::UnsupportedCollation)
            end
          end
        end
      end

      context 'when the view does not have a collation set' do

        let(:filter) do
          { name: 'BANG' }
        end

        before do
          authorized_collection.insert_one(name: 'bang')
        end

        let(:result) do
          view.limit(-1).first
        end

        it 'does not apply the collation' do
          expect(result).to be_nil
        end
      end
    end
  end

  describe '#hash' do

    let(:other) do
      described_class.new(authorized_collection, filter, options)
    end

    it 'returns a unique value based on collection, filter, options' do
      expect(view.hash).to eq(other.hash)
    end

    context 'when two views only have different collections' do

      let(:other_collection) do
        authorized_client[:other]
      end

      let(:other) do
        described_class.new(other_collection, filter, options)
      end

      it 'returns different hash values' do
        expect(view.hash).not_to eq(other.hash)
      end
    end

    context 'when two views only have different filter' do

      let(:other_filter) do
        { 'name' => 'Emily' }
      end

      let(:other) do
        described_class.new(authorized_collection, other_filter, options)
      end

      it 'returns different hash values' do
        expect(view.hash).not_to eq(other.hash)
      end
    end

    context 'when two views only have different options' do

      let(:other_options) do
        { 'limit' => 20 }
      end

      let(:other) do
        described_class.new(authorized_collection, filter, other_options)
      end

      it 'returns different hash values' do
        expect(view.hash).not_to eq(other.hash)
      end
    end
  end

  describe '#initialize' do

    context 'when the filter is not a valid document' do

      let(:filter) do
        'y'
      end

      let(:options) do
        { limit: 5 }
      end

      it 'raises an error' do
        expect do
          view
        end.to raise_error(Mongo::Error::InvalidDocument)
      end
    end

    context 'when the filter and options are standard' do

      let(:filter) do
        { 'name' => 'test' }
      end

      let(:options) do
        { 'sort' => { 'name' => 1 }}
      end

      it 'parses a standard filter' do
        expect(view.filter).to eq(filter)
      end

      it 'parses standard options' do
        expect(view.options).to eq(options)
      end

      it 'only freezes the view filter, not the user filter' do
        expect(view.filter.frozen?).to be(true)
        expect(filter.frozen?).to be(false)
      end

      it 'only freezes the view options, not the user options' do
        expect(view.options.frozen?).to be(true)
        expect(options.frozen?).to be(false)
      end
    end

    context 'when the filter contains modifiers' do

      let(:filter) do
        { :$query => { :name => 'test' }, :$comment => 'testing' }
      end

      let(:options) do
        { :sort => { name: 1 }}
      end

      it 'parses a standard filter' do
        expect(view.filter).to eq('name' => 'test')
      end

      it 'parses standard options' do
        expect(view.options).to eq('sort' => { 'name' => 1 }, 'comment' => 'testing')
      end
    end

    context 'when the options contain modifiers' do

      let(:filter) do
        { 'name' => 'test' }
      end

      let(:options) do
        { :sort => { name: 1 }, :modifiers => { :$comment => 'testing'}}
      end

      it 'parses a standard filter' do
        expect(view.filter).to eq('name' => 'test')
      end

      it 'parses standard options' do
        expect(view.options).to eq('sort' => { 'name' => 1 }, 'comment' => 'testing')
      end
    end

    context 'when the filter and options both contain modifiers' do

      let(:filter) do
        { :$query => { 'name' => 'test' }, :$hint => { name: 1 }}
      end

      let(:options) do
        { :sort => { name: 1 }, :modifiers => { :$comment => 'testing' }}
      end

      it 'parses a standard filter' do
        expect(view.filter).to eq('name' => 'test')
      end

      it 'parses standard options' do
        expect(view.options).to eq(
          'sort' => { 'name' => 1 }, 'comment' => 'testing', 'hint' => { 'name' => 1 }
        )
      end
    end
  end

  describe '#inspect' do

    context 'when there is a namespace, filter, and options' do

      let(:options) do
        { 'limit' => 5 }
      end

      let(:filter) do
        { 'name' => 'Emily' }
      end

      it 'returns a string' do
        expect(view.inspect).to be_a(String)
      end

      it 'returns a string containing the collection namespace' do
        expect(view.inspect).to match(/.*#{authorized_collection.namespace}.*/)
      end

      it 'returns a string containing the filter' do
        expect(view.inspect).to match(/.*#{filter.inspect}.*/)
      end

      it 'returns a string containing the options' do
        expect(view.inspect).to match(/.*#{options.inspect}.*/)
      end
    end
  end
end

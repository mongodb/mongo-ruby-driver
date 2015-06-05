require 'spec_helper'

describe Mongo::Collection::View do

  let(:selector) do
    {}
  end

  let(:options) do
    {}
  end

  let(:view) do
    described_class.new(authorized_collection, selector, options)
  end

  after do
    authorized_collection.find.delete_many
  end

  describe '#==' do

    context 'when the other object is not a collection view' do

      let(:other) { 'test' }

      it 'returns false' do
        expect(view).to_not eq(other)
      end
    end

    context 'when the views have the same collection, selector, and options' do

      let(:other) do
        described_class.new(authorized_collection, selector, options)
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
        described_class.new(other_collection, selector, options)
      end

      it 'returns false' do
        expect(view).not_to eq(other)
      end
    end

    context 'when two views have a different selector' do

      let(:other_selector) do
        { 'name' => 'Emily' }
      end

      let(:other) do
        described_class.new(authorized_collection, other_selector, options)
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
        described_class.new(authorized_collection, selector, other_options)
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

    it 'dups the selector' do
      expect(view.selector).not_to be(view_clone.selector)
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
      authorized_collection.insert_many(documents)
    end

    after do
      authorized_collection.find.delete_many
    end

    context 'when sending the initial query' do

      context 'when limit is specified' do

        let(:options) do
          { :limit => 5 }
        end

        before do
          expect(Mongo::Operation::Read::Query).to receive(:new) do |spec|
            expect(spec[:options][:limit]).to eq(options[:limit])
          end.and_call_original
        end

        let(:returned) do
          view.to_a
        end

        it 'returns limited documents' do
          expect(returned.count).to eq(5)
        end

        it 'allows iteration of the documents' do
          returned.each do |doc|
            expect(doc).to have_key('field')
          end
        end
      end

      context 'when batch size is specified' do

        let(:options) do
          { :batch_size => 5 }
        end

        before do
          expect(Mongo::Operation::Read::Query).to receive(:new) do |spec|
            expect(spec[:options][:limit]).to eq(options[:batch_size])
          end.and_call_original
        end

        let(:returned) do
          view.to_a
        end

        it 'returns all the documents' do
          expect(returned.count).to eq(10)
        end

        it 'allows iteration of all documents' do
          returned.each do |doc|
            expect(doc).to have_key('field')
          end
        end
      end

      context 'when no limit is specified' do

        before do
          expect(Mongo::Operation::Read::Query).to receive(:new) do |spec|
            expect(spec[:options][:limit]).to eq(nil)
          end.and_call_original
        end

        let(:returned) do
          view.to_a
        end

        it 'returns all the documents' do
          expect(returned.count).to eq(10)
        end

        it 'allows iteration of all documents' do
          returned.each do |doc|
            expect(doc).to have_key('field')
          end
        end
      end

      context 'when batch size is greater than limit' do

        let(:options) do
          { :batch_size => 5, :limit => 3 }
        end

        before do
          expect(Mongo::Operation::Read::Query).to receive(:new) do |spec|
            expect(spec[:options][:limit]).to eq(options[:limit])
          end.and_call_original
        end

        let(:returned) do
          view.to_a
        end

        it 'returns the limit of documents' do
          expect(returned.count).to eq(3)
        end

        it 'allows iteration of the documents' do
          returned.each do |doc|
            expect(doc).to have_key('field')
          end
        end
      end

      context 'when limit is greater than batch size' do

        let(:options) do
          { :limit => 5, :batch_size => 3 }
        end

        before do
          expect(Mongo::Operation::Read::Query).to receive(:new) do |spec|
            expect(spec[:options][:limit]).to eq(options[:batch_size])
          end.and_call_original
        end

        let(:returned) do
          view.to_a
        end

        it 'returns the limit of documents' do
          expect(returned.count).to eq(5)
        end

        it 'allows iteration of the documents' do
          returned.each do |doc|
            expect(doc).to have_key('field')
          end
        end
      end

      context 'when the selector has special fields' do

        context 'when a snapshot option is provided' do

          let(:options) do
            { :snapshot => true }
          end

          before do
            expect(Mongo::Operation::Read::Query).to receive(:new) do |spec|
              expect(spec[:selector][:$query]).to eq(selector)
            end.and_call_original
          end

          it 'creates a special query selector' do
            view.each do |doc|
              expect(doc).to have_key('field')
            end
          end
        end

        context 'when a max_scan option is provided' do

          let(:options) do
            { :max_scan => 100 }
          end

          before do
            expect(Mongo::Operation::Read::Query).to receive(:new) do |spec|
              expect(spec[:selector][:$query]).to eq(selector)
            end.and_call_original
          end

          it 'creates a special query selector' do
            view.each do |doc|
              expect(doc).to have_key('field')
            end
          end
        end

        context 'when a show_disk_loc option is provided' do

          let(:options) do
            { :show_disk_loc => true }
          end

          before do
            expect(Mongo::Operation::Read::Query).to receive(:new) do |spec|
              expect(spec[:selector][:$query]).to eq(selector)
            end.and_call_original
          end

          it 'creates a special query selector' do
            view.each do |doc|
              expect(doc).to have_key('field')
              break
            end
          end
        end
      end

      context 'when sorting' do

        let(:options) do
          { :sort => {'x' => Mongo::Index::ASCENDING }}
        end

        before do
          expect(Mongo::Operation::Read::Query).to receive(:new) do |spec|
            expect(spec[:selector][:$query]).to eq(selector)
          end.and_call_original
        end

        it 'creates a special query selector' do
          view.each do |doc|
            expect(doc).to have_key('field')
          end
        end
      end

      context 'when providing a hint' do

        context 'when the hint is bad' do

          let(:options) do
            { :hint => { 'x' => Mongo::Index::ASCENDING }}
          end

          before do
            expect(Mongo::Operation::Read::Query).to receive(:new) do |spec|
              expect(spec[:selector][:$query]).to eq(selector)
            end.and_call_original
          end

          it'creates a special query selector' do
            expect {
              view.to_a
            }.to raise_error(Mongo::Error::OperationFailure)
          end
        end
      end

      context 'when providing a comment' do

        let(:options) do
          { :comment => 'query1' }
        end

        before do
          expect(Mongo::Operation::Read::Query).to receive(:new) do |spec|
            expect(spec[:selector][:$query]).to eq(selector)
          end.and_call_original
        end

        it 'creates a special query selector' do
          view.each do |doc|
            expect(doc).to have_key('field')
          end
        end
      end

      context 'when the cluster is sharded' do

        before do
          expect(Mongo::Operation::Read::Query).to receive(:new) do |spec|
            expect(spec[:selector][:$query]).to eq(selector)
          end.and_call_original
        end

        it 'creates a special query selector' do
          view.each do |doc|
            expect(doc).to have_key('field')
          end
        end
      end
    end

    context 'when there are no special fields' do

      before do
        expect(Mongo::Operation::Read::Query).to receive(:new) do |spec|
          expect(spec[:selector]).to eq(selector)
        end.and_call_original
      end

      it 'creates a normal query spec' do
        view.each do |doc|
          expect(doc).to have_key('field')
        end
      end
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
  end

  describe '#hash' do

    let(:other) do
      described_class.new(authorized_collection, selector, options)
    end

    it 'returns a unique value based on collection, selector, options' do
      expect(view.hash).to eq(other.hash)
    end

    context 'when two views only have different collections' do

      let(:other_collection) do
        authorized_client[:other]
      end

      let(:other) do
        described_class.new(other_collection, selector, options)
      end

      it 'returns different hash values' do
        expect(view.hash).not_to eq(other.hash)
      end
    end

    context 'when two views only have different selectors' do

      let(:other_selector) do
        { 'name' => 'Emily' }
      end

      let(:other) do
        described_class.new(authorized_collection, other_selector, options)
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
        described_class.new(authorized_collection, selector, other_options)
      end

      it 'returns different hash values' do
        expect(view.hash).not_to eq(other.hash)
      end
    end
  end

  describe '#initialize' do

    let(:options) do
      { :limit => 5 }
    end

    it 'sets the collection' do
      expect(view.collection).to eq(authorized_collection)
    end

    it 'sets the selector' do
      expect(view.selector).to eq(selector)
    end

    it 'dups the selector' do
      expect(view.selector).not_to be(selector)
    end

    it 'sets the options' do
      expect(view.options).to eq(options)
    end

    it 'dups the options' do
      expect(view.options).not_to be(options)
    end

    context 'when the selector is not a valid document' do

      let(:selector) do
        'y'
      end

      it 'raises an error' do
        expect do
          view
        end.to raise_error(Mongo::Error::InvalidDocument)
      end
    end
  end

  describe '#inspect' do

    context 'when there is a namespace, selector, and options' do

      let(:options) do
        { :limit => 5 }
      end

      let(:selector) do
        { 'name' => 'Emily' }
      end

      it 'returns a string' do
        expect(view.inspect).to be_a(String)
      end

      it 'returns a string containing the collection namespace' do
        expect(view.inspect).to match(/.*#{authorized_collection.namespace}.*/)
      end

      it 'returns a string containing the selector' do
        expect(view.inspect).to match(/.*#{selector.inspect}.*/)
      end

      it 'returns a string containing the options' do
        expect(view.inspect).to match(/.*#{options.inspect}.*/)
      end
    end
  end
end

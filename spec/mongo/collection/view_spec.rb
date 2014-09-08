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
    authorized_collection.find.remove_many
  end

  pending '#aggregate'
  pending '#map_reduce'
  pending '#parallel_scan'
  pending '#await_data'
  pending '#exhaust'
  pending '#no_cursor_timeout'
  pending '#partial'
  pending '#tailable'

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

  describe '#count' do

    let(:documents) do
      (1..10).map{ |i| { field: "test#{i}" }}
    end

    before do
      authorized_collection.insert_many(documents)
    end

    after do
      authorized_collection.find.remove_many
    end

    context 'when a selector is provided' do

      let(:selector) do
        { field: 'test1' }
      end

      it 'returns the count of matching documents' do
        expect(view.count).to eq(1)
      end
    end

    context 'when no selector is provided' do

      it 'returns the count of matching documents' do
        expect(view.count).to eq(10)
      end
    end
  end

  describe '#distinct' do

    context 'when a selector is provided' do

      let(:selector) do
        { field: 'test' }
      end

      let(:documents) do
        (1..3).map{ |i| { field: "test" }}
      end

      before do
        authorized_collection.insert_many(documents)
      end

      context 'when the field is a symbol' do

        let(:distinct) do
          view.distinct(:field)
        end

        it 'returns the distinct values' do
          expect(distinct).to eq([ 'test' ])
        end
      end

      context 'when the field is a string' do

        let(:distinct) do
          view.distinct('field')
        end

        it 'returns the distinct values' do
          expect(distinct).to eq([ 'test' ])
        end
      end

      context 'when the field is nil' do

        let(:distinct) do
          view.distinct(nil)
        end

        it 'returns an empty array' do
          expect(distinct).to be_empty
        end
      end
    end

    context 'when no selector is provided' do

      let(:documents) do
        (1..3).map{ |i| { field: "test#{i}" }}
      end

      before do
        authorized_collection.insert_many(documents)
      end

      context 'when the field is a symbol' do

        let(:distinct) do
          view.distinct(:field)
        end

        it 'returns the distinct values' do
          expect(distinct).to eq([ 'test1', 'test2', 'test3' ])
        end
      end

      context 'when the field is a string' do

        let(:distinct) do
          view.distinct('field')
        end

        it 'returns the distinct values' do
          expect(distinct).to eq([ 'test1', 'test2', 'test3' ])
        end
      end

      context 'when the field is nil' do

        let(:distinct) do
          view.distinct(nil)
        end

        it 'returns an empty array' do
          expect(distinct).to be_empty
        end
      end
    end
  end

  describe '#explain' do

    let(:explain) do
      view.explain
    end

    it 'executes an explain' do
      expect(explain[:cursor]).to eq('BasicCursor')
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

  describe '#comment' do

    let(:options) do
      { :comment => 'test1' }
    end

    context 'when a comment is specified' do

      let(:new_comment) do
        'test2'
      end

      it 'sets the comment' do
        new_view = view.comment(new_comment)
        expect(new_view.comment).to eq(new_comment)
      end

      it 'returns a new Collection' do
        expect(view.comment(new_comment)).not_to be(view)
      end
    end

    context 'when a comment is not specified' do

      it 'returns the comment' do
        expect(view.comment).to eq(options[:comment])
      end
    end
  end

  describe '#batch_size' do

    let(:options) do
      { :batch_size => 13 }
    end

    context 'when a batch size is specified' do

      let(:new_batch_size) do
        15
      end

      it 'sets the batch size' do
        new_view = view.batch_size(new_batch_size)
        expect(new_view.batch_size).to eq(new_batch_size)
      end

      it 'returns a new Collection' do
        expect(view.batch_size(new_batch_size)).not_to be(view)
      end
    end

    context 'when a batch size is not specified' do

      it 'returns the batch_size' do
        expect(view.batch_size).to eq(options[:batch_size])
      end
    end
  end

  describe '#projection' do

    context 'when projection are specified' do

      let(:options) do
        { :projection => { 'x' => 1 } }
      end

      let(:new_projection) do
        { 'y' => 1 }
      end

      it 'sets the projection' do
        new_view = view.projection(new_projection)
        expect(new_view.projection).to eq(new_projection)
      end

      it 'returns a new Collection' do
        expect(view.projection(new_projection)).not_to be(view)
      end
    end

    context 'when projection are not specified' do

      let(:options) { { :projection => { 'x' => 1 } } }

      it 'returns the projection' do
        expect(view.projection).to eq(options[:projection])
      end
    end
  end

  describe '#hint' do

    context 'when a hint is specified' do

      let(:options) do
        { :hint => { 'x' => Mongo::Index::ASCENDING } }
      end

      let(:new_hint) do
        { 'x' => Mongo::Index::DESCENDING }
      end

      it 'sets the hint' do
        new_view = view.hint(new_hint)
        expect(new_view.hint).to eq(new_hint)
      end

      it 'returns a new Collection' do
        expect(view.hint(new_hint)).not_to be(view)
      end
    end

    context 'when a hint is not specified' do

      let(:options) do
        { :hint => 'x' }
      end

      it 'returns the hint' do
        expect(view.hint).to eq(options[:hint])
      end
    end
  end

  describe '#limit' do

    context 'when a limit is specified' do

      let(:options) do
        { :limit => 5 }
      end

      let(:new_limit) do
        10
      end

      it 'sets the limit' do
        new_view = view.limit(new_limit)
        expect(new_view.limit).to eq(new_limit)
      end

      it 'returns a new Collection' do
        expect(view.limit(new_limit)).not_to be(view)
      end
    end

    context 'when a limit is not specified' do

      let(:options) do
        { :limit => 5 }
      end

      it 'returns the limit' do
        expect(view.limit).to eq(options[:limit])
      end
    end
  end

  describe '#max_scan' do

    let(:new_view) do
      view.max_scan(10)
    end

    it 'sets the value in the options' do
      expect(new_view.max_scan).to eq(10)
    end
  end

  describe '#remove_many' do

    context 'when a selector was provided' do

      let(:selector) do
        { field: 'test1' }
      end

      before do
        authorized_collection.insert_many([{ field: 'test1' }, { field: 'test2' }])
      end

      let(:response) do
        view.remove_many
      end

      it 'deletes the matching documents in the collection' do
        expect(response.n).to eq(1)
      end
    end

    context 'when no selector was provided' do

      before do
        authorized_collection.insert_many([{ field: 'test1' }, { field: 'test2' }])
      end

      let(:response) do
        view.remove_many
      end

      it 'deletes all the documents in the collection' do
        expect(response.n).to eq(2)
      end
    end
  end

  describe '#remove_one' do

    context 'when a selector was provided' do

      let(:selector) do
        { field: 'test1' }
      end

      before do
        authorized_collection.insert_many([
          { field: 'test1' },
          { field: 'test1' },
          { field: 'test1' }
        ])
      end

      let(:response) do
        view.remove_one
      end

      it 'deletes the first matching document in the collection' do
        expect(response.n).to eq(1)
      end
    end

    context 'when no selector was provided' do

      before do
        authorized_collection.insert_many([{ field: 'test1' }, { field: 'test2' }])
      end

      let(:response) do
        view.remove_one
      end

      it 'deletes the first document in the collection' do
        expect(response.n).to eq(1)
      end
    end
  end

  describe '#skip' do

    context 'when a skip is specified' do

      let(:options) do
        { :skip => 5 }
      end

      let(:new_skip) do
        10
      end

      it 'sets the skip value' do
        new_view = view.skip(new_skip)
        expect(new_view.skip).to eq(new_skip)
      end

      it 'returns a new Collection' do
        expect(view.skip(new_skip)).not_to be(view)
      end
    end

    context 'when a skip is not specified' do

      let(:options) do
        { :skip => 5 }
      end

      it 'returns the skip value' do
        expect(view.skip).to eq(options[:skip])
      end
    end
  end

  describe '#read' do

    context 'when a read pref is specified' do

      let(:options) do
        { :read => Mongo::ServerPreference.get(:mode => :secondary) }
      end

      let(:new_read) do
        Mongo::ServerPreference.get(:mode => :secondary_preferred)
      end

      it 'sets the read preference' do
        new_view = view.read(new_read)
        expect(new_view.read).to eq(new_read)
      end

      it 'returns a new Collection' do
        expect(view.read(new_read)).not_to be(view)
      end
    end

    context 'when a read pref is not specified' do

      let(:options) do
        { :read =>  Mongo::ServerPreference.get(:mode => :secondary) }
      end

      it 'returns the read preference' do
        expect(view.read).to eq(options[:read])
      end

      context 'when no read pref is set on initialization' do

        let(:options) do
          {}
        end

        it 'returns the collection read preference' do
          expect(view.read).to eq(authorized_collection.server_preference)
        end
      end
    end
  end

  describe '#show_disk_loc' do

    let(:new_view) do
      view.show_disk_loc(true)
    end

    it 'sets the value in the options' do
      expect(new_view.show_disk_loc).to be true
    end
  end

  describe '#snapshot' do

    let(:new_view) do
      view.snapshot(true)
    end

    it 'sets the value in the options' do
      expect(new_view.snapshot).to be true
    end
  end

  describe '#sort' do

    context 'when a sort is specified' do

      let(:options) do
        { :sort => { 'x' => Mongo::Index::ASCENDING }}
      end

      let(:new_sort) do
        { 'x' => Mongo::Index::DESCENDING }
      end

      it 'sets the sort option' do
        new_view = view.sort(new_sort)
        expect(new_view.sort).to eq(new_sort)
      end

      it 'returns a new Collection' do
        expect(view.sort(new_sort)).not_to be(view)
      end
    end

    context 'when a sort is not specified' do

      let(:options) do
        { :sort => { 'x' => Mongo::Index::ASCENDING }}
      end

      it 'returns the sort' do
        expect(view.sort).to eq(options[:sort])
      end
    end
  end

  describe '#update' do

    context 'when a selector was provided' do

      let(:selector) do
        { field: 'test1' }
      end

      before do
        authorized_collection.insert_many([{ field: 'test1' }, { field: 'test2' }])
      end

      let!(:response) do
        view.update('$set'=> { field: 'testing' })
      end

      let(:updated) do
        authorized_collection.find(field: 'testing').first
      end

      it 'returns the number updated' do
        expect(response.n).to eq(1)
      end

      it 'updates the documents in the collection' do
        expect(updated[:field]).to eq('testing')
      end
    end

    context 'when no selector was provided' do

      before do
        authorized_collection.insert_many([{ field: 'test1' }, { field: 'test2' }])
      end

      let!(:response) do
        view.update('$set'=> { field: 'testing' })
      end

      let(:updated) do
        authorized_collection.find
      end

      it 'returns the number updated' do
        expect(response.n).to eq(2)
      end

      it 'updates all the documents in the collection' do
        updated.each do |doc|
          expect(doc[:field]).to eq('testing')
        end
      end
    end

    context 'when limiting the number updated' do

      context 'when a selector was provided' do

        let(:selector) do
          { field: 'test1' }
        end

        before do
          authorized_collection.insert_many([{ field: 'test1' }, { field: 'test1' }])
        end

        let!(:response) do
          view.limit(1).update('$set'=> { field: 'testing' })
        end

        let(:updated) do
          authorized_collection.find(field: 'testing').first
        end

        it 'updates the first matching document in the collection' do
          expect(response.n).to eq(1)
        end

        it 'updates the documents in the collection' do
          expect(updated[:field]).to eq('testing')
        end
      end

      context 'when no selector was provided' do

        before do
          authorized_collection.insert_many([{ field: 'test1' }, { field: 'test2' }])
        end

        let!(:response) do
          view.limit(1).update('$set'=> { field: 'testing' })
        end

        let(:updated) do
          authorized_collection.find(field: 'testing').first
        end

        it 'updates the first document in the collection' do
          expect(response.n).to eq(1)
        end

        it 'updates the documents in the collection' do
          expect(updated[:field]).to eq('testing')
        end
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
      authorized_collection.find.remove_many
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

        let(:options) do
          { :hint => { 'x' => Mongo::Index::ASCENDING }}
        end

        before do
          expect(Mongo::Operation::Read::Query).to receive(:new) do |spec|
            expect(spec[:selector][:$query]).to eq(selector)
          end.and_call_original
        end

        it'creates a special query selector' do
          view.each do |doc|
            expect(doc).to have_key('$err')
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

    pending 'when the cursor is created'
    pending 'when a block is provided'
    pending 'when a block is not provided'
  end
end

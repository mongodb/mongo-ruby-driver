require 'spec_helper'

describe Mongo::CollectionView do

  let(:selector) do
    {}
  end

  let(:opts) do
    {}
  end

  let(:view) do
    described_class.new(authorized_client[TEST_COLL], selector, opts)
  end

  describe '#initialize' do

    let(:opts) do
      { :limit => 5 }
    end

    it 'sets the collection' do
      expect(view.collection).to eq(authorized_client[TEST_COLL])
    end

    it 'sets the selector' do
      expect(view.selector).to eq(selector)
    end

    it 'dups the selector' do
      expect(view.selector).not_to be(selector)
    end

    it 'sets the options' do
      expect(view.opts).to eq(opts)
    end

    it 'dups the options' do
      expect(view.opts).not_to be(opts)
    end
  end

  describe '#inspect' do

    context 'when there is a namespace, selector, and opts' do

      let(:opts) do
        { :limit => 5 }
      end

      let(:selector) do
        { 'name' => 'Emily' }
      end

      it 'returns a string' do
        expect(view.inspect).to be_a(String)
      end

      it 'returns a string containing the collection namespace' do
        expect(view.inspect).to match(/.*#{authorized_client[TEST_COLL].namespace}.*/)
      end

      it 'returns a string containing the selector' do
        expect(view.inspect).to match(/.*#{selector.inspect}.*/)
      end

      it 'returns a string containing the opts' do
        expect(view.inspect).to match(/.*#{opts.inspect}.*/)
      end
    end
  end

  describe '#comment' do

    let(:opts) do
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

      it 'returns a new CollectionView' do
        expect(view.comment(new_comment)).not_to be(view)
      end
    end

    context 'when a comment is not specified' do

      it 'returns the comment' do
        expect(view.comment).to eq(opts[:comment])
      end
    end
  end

  describe '#batch_size' do

    let(:opts) do
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

      it 'returns a new CollectionView' do
        expect(view.batch_size(new_batch_size)).not_to be(view)
      end
    end

    context 'when a batch size is not specified' do

      it 'returns the batch_size' do
        expect(view.batch_size).to eq(opts[:batch_size])
      end
    end
  end

  describe '#fields' do

    context 'when fields are specified' do

      let(:opts) do
        { :fields => { 'x' => 1 } }
      end

      let(:new_fields) do
        { 'y' => 1 }
      end

      it 'sets the fields' do
        new_view = view.fields(new_fields)
        expect(new_view.fields).to eq(new_fields)
      end

      it 'returns a new CollectionView' do
        expect(view.fields(new_fields)).not_to be(view)
      end
    end

    context 'when fields are not specified' do
      let(:opts) { { :fields => { 'x' => 1 } } }

      it 'returns the fields' do
        expect(view.fields).to eq(opts[:fields])
      end
    end
  end

  describe '#hint' do

    context 'when a hint is specified' do

      let(:opts) do
        { :hint => { 'x' => Mongo::Indexable::ASCENDING } }
      end

      let(:new_hint) do
        { 'x' => Mongo::Indexable::DESCENDING }
      end

      it 'sets the hint' do
        new_view = view.hint(new_hint)
        expect(new_view.hint).to eq(new_hint)
      end

      it 'returns a new CollectionView' do
        expect(view.hint(new_hint)).not_to be(view)
      end
    end

    context 'when a hint is not specified' do

      let(:opts) do
        { :hint => 'x' }
      end

      it 'returns the hint' do
        expect(view.hint).to eq(opts[:hint])
      end
    end
  end

  describe '#limit' do

    context 'when a limit is specified' do

      let(:opts) do
        { :limit => 5 }
      end

      let(:new_limit) do
        10
      end

      it 'sets the limit' do
        new_view = view.limit(new_limit)
        expect(new_view.limit).to eq(new_limit)
      end

      it 'returns a new CollectionView' do
        expect(view.limit(new_limit)).not_to be(view)
      end
    end

    context 'when a limit is not specified' do

      let(:opts) do
        { :limit => 5 }
      end

      it 'returns the limit' do
        expect(view.limit).to eq(opts[:limit])
      end
    end
  end

  describe '#skip' do

    context 'when a skip is specified' do

      let(:opts) do
        { :skip => 5 }
      end

      let(:new_skip) do
        10
      end

      it 'sets the skip value' do
        new_view = view.skip(new_skip)
        expect(new_view.skip).to eq(new_skip)
      end

      it 'returns a new CollectionView' do
        expect(view.skip(new_skip)).not_to be(view)
      end
    end

    context 'when a skip is not specified' do

      let(:opts) do
        { :skip => 5 }
      end

      it 'returns the skip value' do
        expect(view.skip).to eq(opts[:skip])
      end
    end
  end

  describe '#read' do

    context 'when a read pref is specified' do

      let(:opts) do
        { :read => Mongo::ServerPreference.get(:mode => :secondary) }
      end

      let(:new_read) do
        Mongo::ServerPreference.get(:mode => :secondary_preferred)
      end

      it 'sets the read preference' do
        new_view = view.read(new_read)
        expect(new_view.read).to eq(new_read)
      end

      it 'returns a new CollectionView' do
        expect(view.read(new_read)).not_to be(view)
      end
    end

    context 'when a read pref is not specified' do

      let(:opts) do
        { :read =>  Mongo::ServerPreference.get(:mode => :secondary) }
      end

      it 'returns the read preference' do
        expect(view.read).to eq(opts[:read])
      end

      context 'when no read pref is set on initialization' do

        let(:opts) do
          {}
        end

        it 'returns the collection read preference' do
          expect(view.read).to eq(authorized_client[TEST_COLL].server_preference)
        end
      end
    end
  end

  describe '#sort' do

    context 'when a sort is specified' do

      let(:opts) do
        { :sort => { 'x' => Mongo::Indexable::ASCENDING }}
      end

      let(:new_sort) do
        { 'x' => Mongo::Indexable::DESCENDING }
      end

      it 'sets the sort option' do
        new_view = view.sort(new_sort)
        expect(new_view.sort).to eq(new_sort)
      end

      it 'returns a new CollectionView' do
        expect(view.sort(new_sort)).not_to be(view)
      end
    end

    context 'when a sort is not specified' do

      let(:opts) do
        { :sort => { 'x' => Mongo::Indexable::ASCENDING }}
      end

      it 'returns the sort' do
        expect(view.sort).to eq(opts[:sort])
      end
    end
  end

  describe '#special_opts' do

    context 'when special_opts are specified' do

      context 'when snapshot options exist' do

        let(:opts) do
          { :snapshot => true }
        end

        it 'returns snapshot in the special options' do
          expect(view.special_opts).to eq(opts)
        end
      end

      context 'when max_scan options exist' do

        let(:opts) do
          { :max_scan => true }
        end

        it 'returns max_scan in the special options' do
          expect(view.special_opts).to eq(opts)
        end
      end

      context 'when show_disk_loc options exist' do

        let(:opts) do
          { :show_disk_loc => true }
        end

        it 'returns show_disk_loc in the special options' do
          expect(view.special_opts).to eq(opts)
        end
      end

      describe 'when replacing options' do

        let(:opts) do
          { :snapshot => true }
        end

        let(:new_special_opts) do
          { :max_scan => 100 }
        end

        it 'replaces the old special opts' do
          new_view = view.special_opts(new_special_opts)
          expect(new_view.special_opts).to eq(new_special_opts)
        end

        it 'returns a new CollectionView' do
          expect(view.special_opts(new_special_opts)).not_to be(view)
        end
      end
    end

    context 'when special_opts are not specified' do

      let(:opts) do
        { :snapshot => true }
      end

      it 'returns the special opts' do
        expect(view.special_opts).to eq(opts)
      end
    end
  end

  #describe '#count' do
#
  #  it 'calls count on collection' do
  #    allow(collection).to receive(:count).and_return(10)
  #    expect(view.count).to eq(10)
  #  end
  #end
#
  #describe '#explain' do
#
  #  it 'calls explain on collection' do
  #    allow(collection).to receive(:explain) do
  #      { 'n' => 10, 'nscanned' => 11 }
  #    end
  #    expect(view.explain).to eq('n' => 10, 'nscanned' => 11)
  #  end
  #end
#
  #describe '#distinct' do
  #  let(:distinct_stats) { { 'values' => [1], 'stats' => { 'n' => 3 } } }
#
  #  it 'calls distinct on collection' do
  #    allow(collection).to receive(:distinct).and_return(distinct_stats)
  #    expect(view.distinct('name')).to eq(distinct_stats)
  #  end
  #end
#
  describe '#==' do

    context 'when the views have the same collection, selector, and opts' do

      let(:other) do
        described_class.new(authorized_client[TEST_COLL], selector, opts)
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
        described_class.new(other_collection, selector, opts)
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
        described_class.new(authorized_client[TEST_COLL], other_selector, opts)
      end

      it 'returns false' do
        expect(view).not_to eq(other)
      end
    end

    context 'when two views have different opts' do

      let(:other_opts) do
        { 'limit' => 20 }
      end

      let(:other) do
        described_class.new(authorized_client[TEST_COLL], selector, other_opts)
      end

      it 'returns false' do
        expect(view).not_to eq(other)
      end
    end
  end

  describe '#hash' do

    let(:other) do
      described_class.new(authorized_client[TEST_COLL], selector, opts)
    end

    it 'returns a unique value based on collection, selector, opts' do
      expect(view.hash).to eq(other.hash)
    end

    context 'when two views only have different collections' do

      let(:other_collection) do
        authorized_client[:other]
      end

      let(:other) do
        described_class.new(other_collection, selector, opts)
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
        described_class.new(authorized_client[TEST_COLL], other_selector, opts)
      end

      it 'returns different hash values' do
        expect(view.hash).not_to eq(other.hash)
      end
    end

    context 'when two views only have different opts' do

      let(:other_opts) do
        { 'limit' => 20 }
      end

      let(:other) do
        described_class.new(authorized_client[TEST_COLL], selector, other_opts)
      end

      it 'returns different hash values' do
        expect(view.hash).not_to eq(other.hash)
      end
    end
  end

  describe 'copy' do

    let(:view_clone) do
      view.clone
    end

    it 'dups the options' do
      expect(view.opts).not_to be(view_clone.opts)
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
      [
        { field: 'test1' },
        { field: 'test2' },
        { field: 'test3' },
        { field: 'test4' },
        { field: 'test5' },
        { field: 'test6' },
        { field: 'test7' },
        { field: 'test8' },
        { field: 'test9' },
        { field: 'test10' }
      ]
    end

    before do
      authorized_client[TEST_COLL].insert(documents)
    end

    after do
      authorized_client[TEST_COLL].find.remove
    end

    context 'when sending the initial query' do

      context 'when limit is specified' do

        let(:opts) do
          { :limit => 5 }
        end

        before do
          expect(Mongo::Operation::Read::Query).to receive(:new) do |spec|
            expect(spec[:opts][:limit]).to eq(opts[:limit])
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

        let(:opts) do
          { :batch_size => 5 }
        end

        before do
          expect(Mongo::Operation::Read::Query).to receive(:new) do |spec|
            expect(spec[:opts][:limit]).to eq(opts[:batch_size])
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
            expect(spec[:opts][:limit]).to eq(nil)
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

        let(:opts) do
          { :batch_size => 5, :limit => 3 }
        end

        before do
          expect(Mongo::Operation::Read::Query).to receive(:new) do |spec|
            expect(spec[:opts][:limit]).to eq(opts[:limit])
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

        let(:opts) do
          { :limit => 5, :batch_size => 3 }
        end

        before do
          expect(Mongo::Operation::Read::Query).to receive(:new) do |spec|
            expect(spec[:opts][:limit]).to eq(opts[:batch_size])
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

          let(:opts) do
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

          let(:opts) do
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

          let(:opts) do
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

        let(:opts) do
          { :sort => {'x' => Mongo::Indexable::ASCENDING }}
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

        let(:opts) do
          { :hint => { 'x' => Mongo::Indexable::ASCENDING }}
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

        let(:opts) do
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

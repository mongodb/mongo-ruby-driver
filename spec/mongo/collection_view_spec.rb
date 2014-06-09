require 'spec_helper'

describe Mongo::CollectionView do

  include_context 'shared client'

  let(:selector) { {} }
  let(:opts) { {} }

  let(:view) { described_class.new(collection, selector, opts) }

  describe '#initialize' do
    let(:opts) { { :limit => 5 } }

    it 'sets the collection' do
      expect(view.collection).to be(collection)
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

    it 'defaults upsert setting to false' do
      expect(view.upsert).to be(false)
    end
  end

  describe '#inspect' do

    context 'when there is a namespace, selector, and opts' do
      let(:opts) { { :limit => 5 } }
      let(:selector) { { 'name' => 'Emily' } }

      it 'returns a string' do
        expect(view.inspect).to be_a(String)
      end

      it 'returns a string containing the collection namespace' do
        expect(view.inspect).to match(/.*#{collection.full_namespace}.*/)
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
    let(:opts) { { :comment => 'test1' } }

    context 'when a comment is specified' do
      let(:new_comment) { 'test2' }

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

  describe '#comment!' do

    context 'when a comment is specified' do
      let(:opts) { { :comment => 'test1' } }
      let(:new_comment) { 'test2' }

      it 'sets the comment on the same CollectionView' do
        view.comment!(new_comment)
        expect(view.comment).to eq(new_comment)
      end
    end
  end

  describe '#batch_size' do
    let(:opts) { { :batch_size => 13 } }

    context 'when a batch size is specified' do
      let(:new_batch_size) { 15 }

      it 'sets the batch size' do
        new_view = view.batch_size(new_batch_size)
        expect(new_view.batch_size).to eq(new_batch_size)
      end

      it 'retuns a new CollectionView' do
        expect(view.batch_size(new_batch_size)).not_to be(view)
      end
    end

    context 'when a batch size is not specified' do

      it 'returns the batch_size' do
        expect(view.batch_size).to eq(opts[:batch_size])
      end
    end
  end

  describe '#batch_size!' do

    context 'when a batch size is specified' do
      let(:opts) { { :batch_size => 13 } }
      let(:new_batch_size) { 15 }

      it 'sets the batch size on the same CollectionView' do
        view.batch_size!(new_batch_size)
        expect(view.batch_size).to eq(new_batch_size)
      end
    end
  end

  describe '#fields' do

    context 'when fields are specified' do
      let(:opts) { { :fields => { 'x' => 1 } } }
      let(:new_fields) { { 'y' => 1 } }

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

  describe '#fields!' do

    context 'when fields are specified' do
      let(:opts) { { :fields => { 'x' => 1 } } }
      let(:new_fields) { { 'y' => 1 } }

      it 'sets the fields on the same CollectionView' do
        view.fields!(new_fields)
        expect(view.fields).to eq(new_fields)
      end
    end
  end

  describe '#hint' do

    context 'when a hint is specified' do
      let(:opts) { { :hint => { 'x' => ascending } } }
      let(:new_hint) { { 'x' => descending } }

      it 'sets the hint' do
        new_view = view.hint(new_hint)
        expect(new_view.hint).to eq(new_hint)
      end

      it 'returns a new CollectionView' do
        expect(view.hint(new_hint)).not_to be(view)
      end
    end

    context 'when a hint is not specified' do
      let(:opts) { { :hint => 'x' } }

      it 'returns the hint' do
        expect(view.hint).to eq(opts[:hint])
      end
    end
  end

  describe '#hint!' do

    context 'when a hint is specified' do
      let(:opts) { { :hint => { 'x' => ascending } } }
      let(:new_hint) { { 'x' => descending } }

      it 'sets the hint on the same CollectionView' do
        view.hint!(new_hint)
        expect(view.hint).to eq(new_hint)
      end
    end
  end

  describe '#limit' do

    context 'when a limit is specified' do
      let(:opts) { { :limit => 5 } }
      let(:new_limit) { 10 }

      it 'sets the limit' do
        new_view = view.limit(new_limit)
        expect(new_view.limit).to eq(new_limit)
      end

      it 'returns a new CollectionView' do
        expect(view.limit(new_limit)).not_to be(view)
      end
    end

    context 'when a limit is not specified' do
      let(:opts) { { :limit => 5 } }

      it 'returns the limit' do
        expect(view.limit).to eq(opts[:limit])
      end
    end
  end

  describe '#limit!' do

    context 'when a limit is specified' do
      let(:opts) { { :limit => 5 } }
      let(:new_limit) { 10 }

      it 'sets the limit on the same CollectionView' do
        view.limit!(new_limit)
        expect(view.limit).to eq(new_limit)
      end
    end
  end

  describe '#skip' do

    context 'when a skip is specified' do
      let(:opts) { { :skip => 5 } }
      let(:new_skip) { 10 }

      it 'sets the skip value' do
        new_view = view.skip(new_skip)
        expect(new_view.skip).to eq(new_skip)
      end

      it 'returns a new CollectionView' do
        expect(view.skip(new_skip)).not_to be(view)
      end
    end

    context 'when a skip is not specified' do
      let(:opts) { { :skip => 5 } }

      it 'returns the skip value' do
        expect(view.skip).to eq(opts[:skip])
      end
    end
  end

  describe '#skip!' do

    context 'when a skip is specified' do
      let(:opts) { { :skip => 5 } }
      let(:new_skip) { 10 }

      it 'sets the skip value on the same CollectionView' do
        view.skip!(new_skip)
        expect(view.skip).to eq(new_skip)
      end
    end
  end

  describe '#read' do

    context 'when a read pref is specified' do
      let(:opts) { { :read =>  :secondary } }
      let(:new_read) { :secondary_preferred }

      it 'sets the read preference' do
        new_view = view.read(new_read)
        expect(new_view.read).to eq(new_read)
      end

      it 'returns a new CollectionView' do
        expect(view.read(new_read)).not_to be(view)
      end
    end

    context 'when a read pref is not specified' do
      let(:opts) { { :read => :secondary } }

      it 'returns the read preference' do
        expect(view.read).to eq(opts[:read])
      end

      context 'when no read pref is set on initialization' do
        let(:opts) { {} }

        it 'returns the collection read preference' do
          expect(view.read).to eq(collection.read)
        end
      end
    end
  end

  describe '#read!' do

    context 'when a read pref is specified' do
      let(:opts) { { :read =>  :secondary } }
      let(:new_read) { :secondary_preferred }

      it 'sets the read preference on the same CollectionView' do
        view.read!(new_read)
        expect(view.read).to eq(new_read)
      end
    end
  end

  describe '#write_concern' do
    let(:opts) { { :write_concern => Mongo::WriteConcern::Acknowledged } }
    let(:new_write_concern) { Mongo::WriteConcern::Unacknowledged }

    context 'when a write concern is specified' do

      it 'sets the write concern value' do
        new_view = view.write_concern(new_write_concern)
        expect(new_view.write_concern).to eq(new_write_concern)
      end

      it 'returns a new CollectionView' do
        expect(view.write_concern(new_write_concern)).not_to be(view)
      end
    end

    context 'when a write concern is not specified' do

      it 'returns the write concern value' do
        expect(view.write_concern).to eq(opts[:write_concern])
      end
    end
  end

  describe '#write_concern!' do
    let(:opts) { { :write_concern => Mongo::WriteConcern::Acknowledged } }
    let(:new_write_concern) { Mongo::WriteConcern::Unacknowledged }

    context 'when a write concern is specified' do

      it 'sets the write concern value on the same CollectionView' do
        view.write_concern!(new_write_concern)
        expect(view.write_concern).to eq(new_write_concern)
      end
    end
  end

  describe '#sort' do

    context 'when a sort is specified' do
      let(:opts) { { 'x' => ascending } }
      let(:new_sort) { { 'x' => descending }  }

      it 'sets the sort option' do
        new_view = view.sort(new_sort)
        expect(new_view.sort).to eq(new_sort)
      end

      it 'returns a new CollectionView' do
        expect(view.sort(new_sort)).not_to be(view)
      end
    end

    context 'when a sort is not specified' do
      let(:opts) { { 'x' => ascending } }

      it 'returns the sort' do
        expect(view.sort).to eq(opts[:sort])
      end
    end
  end

  describe '#sort!' do

    context 'when a sort is specified' do
      let(:opts) { { 'x' => ascending } }
      let(:new_sort) { { 'x' => descending }  }

      it 'sets the sort option on the same CollectionView' do
        view.sort!(new_sort)
        expect(view.sort).to eq(new_sort)
      end
    end
  end

  describe '#upsert' do

    context 'when upsert is set' do
      let(:opts) { { :upsert => false } }
      let(:new_upsert) { true }

      it 'sets the upsert option' do
        new_view = view.upsert(new_upsert)
        expect(new_view.upsert).to eq(new_upsert)
      end

      it 'returns a new CollectionView' do
        expect(view.upsert(new_upsert)).not_to be(view)
      end
    end

    context 'when a upsert value is not specified' do
      let(:opts) { { :upsert => true } }

      it 'returns the upsert setting' do
        expect(view.upsert).to eq(opts[:upsert])
      end
    end
  end

  describe '#upsert!' do

    context 'when an upsert setting is specified' do
      let(:opts) { { :upsert => false } }
      let(:new_upsert) { true }

      it 'sets the upsert option on the same CollectionView' do
        view.upsert!(new_upsert)
        expect(view.upsert).to eq(new_upsert)
      end
    end
  end

  describe '#query_opts' do

    context 'when query_opts are specified' do
      context 'snapshot' do
        let(:opts) { { :snapshot => true } }

        it 'returns shapshot in the query options' do
          expect(view.query_opts).to eq(opts)
        end
      end

      context 'max_scan' do
        let(:opts) { { :max_scan => true } }

        it 'returns max_scan in the query options' do
          expect(view.query_opts).to eq(opts)
        end
      end

      context 'show_disk_loc' do
        let(:opts) { { :show_disk_loc => true } }

        it 'returns show_disk_loc in the query options' do
          expect(view.query_opts).to eq(opts)
        end
      end

      describe 'replacement' do
        let(:opts) { { :snapshot => true } }
        let(:new_query_opts) { { :max_scan => 100 }  }

        it 'replaces the old query opts' do
          new_view = view.query_opts(new_query_opts)
          expect(new_view.query_opts).to eq(new_query_opts)
        end
      end

      describe 'immutability' do
        let(:new_query_opts) { { :max_scan => 100 }  }

        it 'returns a new CollectionView' do
          expect(view.query_opts(new_query_opts)).not_to be(view)
        end
      end
    end

    context 'when query_opts are not specified' do
      let(:opts) { { :snapshot => true } }

      it 'returns the query opts' do
        expect(view.query_opts).to eq(opts)
      end
    end
  end

  describe '#query_opts!' do

    context 'when query_opts are specified' do
      let(:opts) { { :snapshot => true } }
      let(:new_query_opts) { { :max_scan => 100, :snapshot => false } }

      it 'sets the query options on the same CollectionView' do
        view.query_opts!(new_query_opts)
        expect(view.query_opts).to eq(new_query_opts)
      end
    end
  end

  describe '#count' do

    it 'calls count on collection' do
      allow(collection).to receive(:count).and_return(10)
      expect(view.count).to eq(10)
    end
  end

  describe '#explain' do

    it 'calls explain on collection' do
      allow(collection).to receive(:explain) do
        { 'n' => 10, 'nscanned' => 11 }
      end
      expect(view.explain).to eq('n' => 10, 'nscanned' => 11)
    end
  end

  describe '#distinct' do
    let(:distinct_stats) { { 'values' => [1], 'stats' => { 'n' => 3 } } }

    it 'calls distinct on collection' do
      allow(collection).to receive(:distinct).and_return(distinct_stats)
      expect(view.distinct('name')).to eq(distinct_stats)
    end
  end

  describe '#==' do

    context 'when the views have the same collection, selector, and opts' do
      let(:other) { described_class.new(collection, selector, opts) }

      it 'returns true' do
        expect(view).to eq(other)
      end
    end

    context 'when two views have a different collection' do
      let(:other_collection) { double('collection') }
      let(:other) { described_class.new(other_collection, selector, opts) }

      it 'returns false' do
        expect(view).not_to eq(other)
      end
    end

    context 'when two views have a different selector' do
      let(:other_selector) { { 'name' => 'Emily' } }
      let(:other) { described_class.new(collection, other_selector, opts) }

      it 'returns false' do
        expect(view).not_to eq(other)
      end
    end

    context 'when two views have different opts' do
      let(:other_opts) { { 'limit' => 20 } }
      let(:other) { described_class.new(collection, selector, other_opts) }

      it 'returns false' do
        expect(view).not_to eq(other)
      end
    end
  end

  describe '#hash' do
    let(:other) { described_class.new(collection, selector, opts) }

    it 'returns a unique value based on collection, selector, opts' do
      expect(view.hash).to eq(other.hash)
    end

    context 'when two views only have different collections' do
      let(:other_collection) { double('collection') }
      let(:other) { described_class.new(other_collection, selector, opts) }

      it 'returns different hash values' do
        allow(other_collection).to receive(:full_namespace) do
          "#{TEST_DB}.OTHER_COLL"
        end
        expect(view.hash).not_to eq(other.hash)
      end
    end

    context 'when two views only have different selectors' do
      let(:other_selector) { { 'name' => 'Emily' } }
      let(:other) { described_class.new(collection, other_selector, opts) }

      it 'returns different hash values' do
        expect(view.hash).not_to eq(other.hash)
      end
    end

    context 'when two views only have different opts' do
      let(:other_opts) { { 'limit' => 20 } }
      let(:other) { described_class.new(collection, selector, other_opts) }

      it 'returns different hash values' do
        expect(view.hash).not_to eq(other.hash)
      end
    end
  end

  describe 'copy' do
    let(:view_clone) { view.clone }

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

  describe 'enumerable' do
    let(:n_docs) { 5 }
    let(:results) do
      { :cursor_id => 0,
        :nreturned => n_docs,
        :docs => (0...n_docs).to_a
      }
    end
    before(:each) do
      allow(connection).to receive(:send_and_receive) { [results, server] }
    end

    describe '#each' do

      context 'when a block is provided' do

        it 'yields each doc to the block' do
          expect { |b| view.each(&b) }.to yield_control.exactly(n_docs).times
        end
      end

      context 'when a block is not provided' do

        it 'returns an enumerator' do
          if !defined?(Enumerator)
            expect(view.each).to be_a(Enumerable::Enumerator)
          else
            expect(view.each).to be_a(Enumerator)
          end
        end
      end
    end
  end

  describe 'chaining' do

    context 'when helper methods are chained' do

      it 'alters the view' do
        new_view = view.limit(5).skip(10)
        expect(new_view.limit).to eq(5)
        expect(new_view.skip).to eq(10)
      end
    end

    context 'when a view is chained with a terminator' do
      include_context 'shared cursor'

      describe '#count' do
        it 'terminates the chaining and returns a value' do
          allow(collection).to receive(:count).and_return(10)
          expect(view.limit(5).skip(10).count).to eq(10)
        end
      end

      describe '#to_a' do
        it 'terminates chaining by returning an array of results' do
          allow(connection).to receive(:send_and_receive).and_return(responses)
          expect(view.limit(5).skip(10).to_a).to eq(results.first[:docs])
        end
      end
    end
  end
end

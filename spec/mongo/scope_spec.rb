require 'spec_helper'

describe Mongo::Scope do

  include_context 'shared client'

  let(:selector) { {} }
  let(:opts) { {} }

  let(:scope) { described_class.new(collection, selector, opts) }

  describe '#initialize' do
    let(:opts) { { :limit => 5 } }

    it 'sets the collection' do
      expect(scope.collection).to be(collection)
    end

    it 'sets the selector' do
      expect(scope.selector).to eq(selector)
    end

    it 'dups the selector' do
      expect(scope.selector).not_to be(selector)
    end

    it 'sets the options' do
      expect(scope.opts).to eq(opts)
    end

    it 'dups the options' do
      expect(scope.opts).not_to be(opts)
    end
  end

  describe '#inspect' do

    context 'when there is a namespace, selector, and opts' do
      let(:opts) { { :limit => 5 } }
      let(:selector) { { 'name' => 'Emily' } }

      it 'returns a string' do
        expect(scope.inspect).to be_a(String)
      end

      it 'returns a string containing the collection namespace' do
        expect(scope.inspect).to match(/.*#{collection.full_namespace}.*/)
      end

      it 'returns a string containing the selector' do
        expect(scope.inspect).to match(/.*#{selector.inspect}.*/)
      end

      it 'returns a string containing the opts' do
        expect(scope.inspect).to match(/.*#{opts.inspect}.*/)
      end
    end
  end

  describe '#comment' do
    let(:opts) { { :comment => 'test1' } }

    context 'when a comment is specified' do
      let(:new_comment) { 'test2' }

      it 'sets the comment' do
        new_scope = scope.comment(new_comment)
        expect(new_scope.comment).to eq(new_comment)
      end

      it 'returns a new Scope' do
        expect(scope.comment(new_comment)).not_to be(scope)
      end
    end

    context 'when a comment is not specified' do

      it 'returns the comment' do
        expect(scope.comment).to eq(opts[:comment])
      end
    end
  end

  describe '#comment!' do

    context 'when a comment is specified' do
      let(:opts) { { :comment => 'test1' } }
      let(:new_comment) { 'test2' }

      it 'sets the comment on the same Scope' do
        scope.comment!(new_comment)
        expect(scope.comment).to eq(new_comment)
      end
    end
  end

  describe '#batch_size' do
    let(:opts) { { :batch_size => 13 } }

    context 'when a batch size is specified' do
      let(:new_batch_size) { 15 }

      it 'sets the batch size' do
        new_scope = scope.batch_size(new_batch_size)
        expect(new_scope.batch_size).to eq(new_batch_size)
      end

      it 'retuns a new scope' do
        expect(scope.batch_size(new_batch_size)).not_to be(scope)
      end
    end

    context 'when a batch size is not specified' do

      it 'returns the batch_size' do
        expect(scope.batch_size).to eq(opts[:batch_size])
      end
    end
  end

  describe '#batch_size!' do

    context 'when a batch size is specified' do
      let(:opts) { { :batch_size => 13 } }
      let(:new_batch_size) { 15 }

      it 'sets the batch size on the same Scope' do
        scope.batch_size!(new_batch_size)
        expect(scope.batch_size).to eq(new_batch_size)
      end
    end
  end

  describe '#fields' do

    context 'when fields are specified' do
      let(:opts) { { :fields => { 'x' => 1 } } }
      let(:new_fields) { { 'y' => 1 } }

      it 'sets the fields' do
        new_scope = scope.fields(new_fields)
        expect(new_scope.fields).to eq(new_fields)
      end

      it 'returns a new Scope' do
        expect(scope.fields(new_fields)).not_to be(scope)
      end
    end

    context 'when fields are not specified' do
      let(:opts) { { :fields => { 'x' => 1 } } }

      it 'returns the fields' do
        expect(scope.fields).to eq(opts[:fields])
      end
    end
  end

  describe '#fields!' do

    context 'when fields are specified' do
      let(:opts) { { :fields => { 'x' => 1 } } }
      let(:new_fields) { { 'y' => 1 } }

      it 'sets the fields on the same Scope' do
        scope.fields!(new_fields)
        expect(scope.fields).to eq(new_fields)
      end
    end
  end

  describe '#hint' do

    context 'when a hint is specified' do
      let(:opts) { { :hint => { 'x' => ascending } } }
      let(:new_hint) { { 'x' => descending } }

      it 'sets the hint' do
        new_scope = scope.hint(new_hint)
        expect(new_scope.hint).to eq(new_hint)
      end

      it 'returns a new Scope' do
        expect(scope.hint(new_hint)).not_to be(scope)
      end
    end

    context 'when a hint is not specified' do
      let(:opts) { { :hint => 'x' } }

      it 'returns the hint' do
        expect(scope.hint).to eq(opts[:hint])
      end
    end
  end

  describe '#hint!' do

    context 'when a hint is specified' do
      let(:opts) { { :hint => { 'x' => ascending } } }
      let(:new_hint) { { 'x' => descending } }

      it 'sets the hint on the same Scope' do
        scope.hint!(new_hint)
        expect(scope.hint).to eq(new_hint)
      end
    end
  end

  describe '#limit' do

    context 'when a limit is specified' do
      let(:opts) { { :limit => 5 } }
      let(:new_limit) { 10 }

      it 'sets the limit' do
        new_scope = scope.limit(new_limit)
        expect(new_scope.limit).to eq(new_limit)
      end

      it 'returns a new Scope' do
        expect(scope.limit(new_limit)).not_to be(scope)
      end
    end

    context 'when a limit is not specified' do
      let(:opts) { { :limit => 5 } }

      it 'returns the limit' do
        expect(scope.limit).to eq(opts[:limit])
      end
    end
  end

  describe '#limit!' do

    context 'when a limit is specified' do
      let(:opts) { { :limit => 5 } }
      let(:new_limit) { 10 }

      it 'sets the limit on the same Scope' do
        scope.limit!(new_limit)
        expect(scope.limit).to eq(new_limit)
      end
    end
  end

  describe '#skip' do

    context 'when a skip is specified' do
      let(:opts) { { :skip => 5 } }
      let(:new_skip) { 10 }

      it 'sets the skip value' do
        new_scope = scope.skip(new_skip)
        expect(new_scope.skip).to eq(new_skip)
      end

      it 'returns a new Scope' do
        expect(scope.skip(new_skip)).not_to be(scope)
      end
    end

    context 'when a skip is not specified' do
      let(:opts) { { :skip => 5 } }

      it 'returns the skip value' do
        expect(scope.skip).to eq(opts[:skip])
      end
    end
  end

  describe '#skip!' do

    context 'when a skip is specified' do
      let(:opts) { { :skip => 5 } }
      let(:new_skip) { 10 }

      it 'sets the skip value on the same Scope' do
        scope.skip!(new_skip)
        expect(scope.skip).to eq(new_skip)
      end
    end
  end

  describe '#read' do

    context 'when a read pref is specified' do
      let(:opts) { { :read =>  :secondary } }
      let(:new_read) { :secondary_preferred }

      it 'sets the read preference' do
        new_scope = scope.read(new_read)
        expect(new_scope.read).to eq(new_read)
      end

      it 'returns a new Scope' do
        expect(scope.read(new_read)).not_to be(scope)
      end
    end

    context 'when a read pref is not specified' do
      let(:opts) { { :read => :secondary } }

      it 'returns the read preference' do
        expect(scope.read).to eq(opts[:read])
      end

      context 'when no read pref is set on initializaiton' do
        let(:opts) { {} }

        it 'returns the collection read preference' do
          expect(scope.read).to eq(collection.read)
        end
      end
    end
  end

  describe '#read!' do

    context 'when a read pref is specified' do
      let(:opts) { { :read =>  :secondary } }
      let(:new_read) { :secondary_preferred }

      it 'sets the read preference on the same Scope' do
        scope.read!(new_read)
        expect(scope.read).to eq(new_read)
      end
    end
  end

  describe '#sort' do

    context 'when a sort is specified' do
      let(:opts) { { 'x' => ascending } }
      let(:new_sort) { { 'x' => descending }  }

      it 'sets the sort option' do
        new_scope = scope.sort(new_sort)
        expect(new_scope.sort).to eq(new_sort)
      end

      it 'returns a new Scope' do
        expect(scope.sort(new_sort)).not_to be(scope)
      end
    end

    context 'when a sort is not specified' do
      let(:opts) { { 'x' => ascending } }

      it 'returns the sort' do
        expect(scope.sort).to eq(opts[:sort])
      end
    end
  end

  describe '#sort!' do

    context 'when a sort is specified' do
      let(:opts) { { 'x' => ascending } }
      let(:new_sort) { { 'x' => descending }  }

      it 'sets the sort option on the same Scope' do
        scope.sort!(new_sort)
        expect(scope.sort).to eq(new_sort)
      end
    end
  end

  describe '#query_opts' do

    context 'when query_opts are specified' do
      context 'snapshot' do
        let(:opts) { { :snapshot => true } }

        it 'returns shapshot in the query options' do
          expect(scope.query_opts).to eq(opts)
        end
      end

      context 'max_scan' do
        let(:opts) { { :max_scan => true } }

        it 'returns max_scan in the query options' do
          expect(scope.query_opts).to eq(opts)
        end
      end

      context 'show_disk_loc' do
        let(:opts) { { :show_disk_loc => true } }

        it 'returns show_disk_loc in the query options' do
          expect(scope.query_opts).to eq(opts)
        end
      end

      describe 'replacement' do
        let(:opts) { { :snapshot => true } }
        let(:new_query_opts) { { :max_scan => 100 }  }

        it 'replaces the old query opts' do
          new_scope = scope.query_opts(new_query_opts)
          expect(new_scope.query_opts).to eq(new_query_opts)
        end
      end

      describe 'immutability' do
        let(:new_query_opts) { { :max_scan => 100 }  }

        it 'returns a new Scope' do
          expect(scope.query_opts(new_query_opts)).not_to be(scope)
        end
      end
    end

    context 'when query_opts are not specified' do
      let(:opts) { { :snapshot => true } }

      it 'returns the query opts' do
        expect(scope.query_opts).to eq(opts)
      end
    end
  end

  describe '#query_opts!' do

    context 'when query_opts are specified' do
      let(:opts) { { :snapshot => true } }
      let(:new_query_opts) { { :max_scan => 100, :snapshot => false } }

      it 'sets the query options on the same Scope' do
        scope.query_opts!(new_query_opts)
        expect(scope.query_opts).to eq(new_query_opts)
      end
    end
  end

  describe '#count' do

    it 'calls count on collection' do
      allow(collection).to receive(:count).and_return(10)
      expect(scope.count).to eq(10)
    end
  end

  describe '#explain' do

    it 'calls explain on collection' do
      allow(collection).to receive(:explain) do
        { 'n' => 10, 'nscanned' => 11 }
      end
      expect(scope.explain).to eq('n' => 10, 'nscanned' => 11)
    end
  end

  describe '#distinct' do
    let(:distinct_stats) { { 'values' => [1], 'stats' => { 'n' => 3 } } }

    it 'calls distinct on collection' do
      allow(collection).to receive(:distinct).and_return(distinct_stats)
      expect(scope.distinct('name')).to eq(distinct_stats)
    end
  end

  describe '#==' do

    context 'when the scopes have the same collection, selector, and opts' do
      let(:other) { described_class.new(collection, selector, opts) }

      it 'returns true' do
        expect(scope).to eq(other)
      end
    end

    context 'when two scopes have a different collection' do
      let(:other_collection) { double('collection') }
      let(:other) { described_class.new(other_collection, selector, opts) }

      it 'returns false' do
        expect(scope).not_to eq(other)
      end
    end

    context 'when two scopes have a different selector' do
      let(:other_selector) { { 'name' => 'Emily' } }
      let(:other) { described_class.new(collection, other_selector, opts) }

      it 'returns false' do
        expect(scope).not_to eq(other)
      end
    end

    context 'when two scopes have different opts' do
      let(:other_opts) { { 'limit' => 20 } }
      let(:other) { described_class.new(collection, selector, other_opts) }

      it 'returns false' do
        expect(scope).not_to eq(other)
      end
    end
  end

  describe '#hash' do
    let(:other) { described_class.new(collection, selector, opts) }

    it 'returns a unique value based on collection, selector, opts' do
      expect(scope.hash).to eq(other.hash)
    end

    context 'when two scopes only have different collections' do
      let(:other_collection) { double('collection') }
      let(:other) { described_class.new(other_collection, selector, opts) }

      it 'returns different hash values' do
        allow(other_collection).to receive(:full_namespace) do
          "#{TEST_DB}.OTHER_COLL"
        end
        expect(scope.hash).not_to eq(other.hash)
      end
    end

    context 'when two scopes only have different selectors' do
      let(:other_selector) { { 'name' => 'Emily' } }
      let(:other) { described_class.new(collection, other_selector, opts) }

      it 'returns different hash values' do
        expect(scope.hash).not_to eq(other.hash)
      end
    end

    context 'when two scopes only have different opts' do
      let(:other_opts) { { 'limit' => 20 } }
      let(:other) { described_class.new(collection, selector, other_opts) }

      it 'returns different hash values' do
        expect(scope.hash).not_to eq(other.hash)
      end
    end
  end

  describe 'copy' do
    let(:scope_clone) { scope.clone }

    it 'dups the options' do
      expect(scope.opts).not_to be(scope_clone.opts)
    end

    it 'dups the selector' do
      expect(scope.selector).not_to be(scope_clone.selector)
    end

    it 'references the same collection' do
      expect(scope.collection).to be(scope_clone.collection)
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
      allow(connection).to receive(:send_and_receive) { [results, node] }
    end

    describe '#each' do

      context 'when a block is provided' do

        it 'yields each doc to the block' do
          expect { |b| scope.each(&b) }.to yield_control.exactly(n_docs).times
        end
      end

      context 'when a block is not provided' do

        it 'returns an enumerator' do
          if !defined?(Enumerator)
            expect(scope.each).to be_a(Enumerable::Enumerator)
          else
            expect(scope.each).to be_a(Enumerator)
          end
        end
      end
    end
  end

  describe 'chaining' do

    context 'when helper methods are chained' do

      it 'alters the scope' do
        new_scope = scope.limit(5).skip(10)
        expect(new_scope.limit).to eq(5)
        expect(new_scope.skip).to eq(10)
      end
    end

    context 'when a scope is chained with a terminator' do
      include_context 'shared cursor'

      describe '#count' do
        it 'terminates the chaining and returns a value' do
          allow(collection).to receive(:count).and_return(10)
          expect(scope.limit(5).skip(10).count).to eq(10)
        end
      end

      describe '#to_a' do
        it 'terminates chaining by returning an array of results' do
          allow(connection).to receive(:send_and_receive).and_return(responses)
          expect(scope.limit(5).skip(10).to_a).to eq(results.first[:docs])
        end
      end
    end
  end
end

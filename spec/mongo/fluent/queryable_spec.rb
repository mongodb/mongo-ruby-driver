require 'spec_helper'

describe Mongo::CollectionView do
  include_context 'shared client'

  let(:selector) { { :name => 'Sam' } }
  let(:view) { described_class.new(collection, selector) }

  describe '#distinct' do

    context 'when limit has been specified' do

      context 'when limit is 1' do

        it 'runs the query' do
          #expect(view.limit(1).distinct(:name)).to be_a(Hash)
        end
      end

      context 'when limit is greater than 1' do

        it 'raises an exception' do
          expect{ view.limit(10).distinct(:name) }.to raise_error
        end
      end
    end

    context 'when skip has been specified' do

      it 'raises an exception' do
        expect{ view.skip(2).distinct(:name) }.to raise_error
      end
    end

    context 'distinct operation alone' do

      it 'runs the distinct query' do
        # todo
      end

      it 'returns a hash' do
        #expect(view.distinct(:name)).to be_a(Hash)
      end
    end
  end

  describe '#explain' do

    it 'executes an explain operation' do
      # todo
    end

    it 'returns a hash' do
      #expect(view.explain).to be_a(Hash)
    end
  end

  describe '#fetch_one' do

    it 'creates a query operation with the correct spec' do
      # todo
    end

    it 'executes a find query' do
      # todo
    end

    it 'returns a hash' do
      #expect(view.fetch_one).to be_a(Hash)
    end
  end
end

require 'spec_helper'

describe Mongo::CollectionView do
  include_context 'shared client'

  let(:view) { described_class.new(collection, {:name => 'Emily'})}

  describe '#remove' do

    context 'when sort has been specified' do

      it 'raises an exception' do
        expect{ view.sort({ :a => 1}).remove }.to raise_error
      end
    end

    context 'when skip has been specified' do

      it 'raises an exception' do
        expect{ view.skip(2).remove }.to raise_error
      end
    end

    context 'when limit other than 1 has been specified' do

      it 'raises an exception' do
        #expect(view.limit(2).remove)
      end
    end
  end
end

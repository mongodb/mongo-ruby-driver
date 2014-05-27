require 'spec_helper'

describe Mongo::CollectionView do
  include_context 'shared client'

  let(:selector) { { :name => 'Emily' } }
  let(:view) { described_class.new(collection, selector) }

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
        expect{ view.limit(2).remove }.to raise_error
      end
    end

    context 'delete operation' do

      it 'creates a delete operation with the correct spec' do
        #expect(Mongo::Operation::Delete).to receive(:new)
      end

      it 'executes a delete operation' do
        #expect_any_instance_of(Mongo::Operation::Delete).to receive(:execute)
      end
    end
  end
end

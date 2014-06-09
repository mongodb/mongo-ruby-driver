require 'spec_helper'

describe Mongo::CollectionView do
  include_context 'shared client'

  let(:selector) { { :name => 'Sam' } }
  let(:view) { described_class.new(collection, selector) }
  let(:update) { { '$set' => { :name => 'Sam' } } }
  let(:replacement) { { :name => 'Sam' } }

  describe '#validate_update!' do

    context 'the first key in the document begins with $' do

      it 'does not raise an exception' do
        expect{ view.validate_update!(update) }.to_not raise_error
      end
    end

    context 'the first key in the document does not begin with $' do

      it 'raises an exception' do
        expect{ view.validate_update!(replacement) }.to raise_error
      end
    end
  end

  describe '#validate_replacement!' do

    context 'the first key in the document begins with $' do

      it 'raises an exception' do
        expect{ view.validate_replacement!(update) }.to raise_error
      end
    end

    context 'the first key in the document does not begin with $' do

      it 'does not raise an exception' do
        expect do
          view.validate_replacement!(replacement)
        end.to_not raise_error
      end
    end
  end
end

require 'spec_helper'

describe Mongo::CollectionView do
  include_context 'shared client'

  let(:selector) { { :name => 'Sam' } }
  let(:view) { described_class.new(collection, selector) }
  let(:update_doc) { { '$set' => { :name => 'Sam' } } }
  let(:replace_doc) { { :name => 'Sam' } }

  describe '#validate_update!' do

    context 'the first key in the document begins with $' do

      it 'does nothing' do
        expect{ view.validate_update!(update_doc) }.to_not raise_error
      end
    end

    context 'the first key in the document does not begin with $' do

      it 'raises and exception' do
        expect{ view.validate_update!(replace_doc) }.to raise_error
      end
    end
  end

  describe '#validate_replacement!' do

    context 'the document has keys beginning with $' do

      it 'raises an exception' do
        expect{ view.validate_replacement!(update_doc) }.to raise_error
      end
    end

    context 'the document has no keys beginning with $' do

      it 'does nothing' do
        expect{ view.validate_replacement!(replace_doc) }.to_not raise_error
      end
    end
  end
end

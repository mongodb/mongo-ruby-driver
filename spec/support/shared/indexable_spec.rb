shared_context 'indexed' do
  let(:spec)  { { :name => 1 } }
  let(:opts)  { { :unique => true } }
  let(:index) { described_class.new(spec, collection, opts) }
end

shared_examples 'an indexed collection' do

  # @todo - these examples can only really be filled in once collection works.

  describe '#create_index' do

    it 'creates an index' do
    end
  end

  describe '#drop_index' do

    it 'removes the specified index' do
    end

    it 'leaves other indexes in the collection' do
    end

    it 'leaves data in the collection' do
    end
  end

  describe '#drop_indexes' do

    it 'removes all indexes from the collection' do
    end
  end

  describe '#ensure_index' do

    context 'the index is a new index' do

      it 'creates the index on the collection' do
      end
    end

    context 'the index has been applied recently' do

      it 'does not create the index on the collection' do
      end
    end

    context 'the index was applied, but not recently' do

      it 'creates the index on the collection' do
      end
    end

    context 'the index is new to us but exists in the collection' do

      it 'creates the index on the collection' do
      end
    end
  end
end

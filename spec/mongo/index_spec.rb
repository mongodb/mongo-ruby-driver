require 'spec_helper'

describe Mongo::Index do

  let(:client)         { Mongo::Client.new(['127.0.0.1:27017']) }
  let(:db)             { Mongo::Database.new(client, 'test') }
  let(:collection)     { Mongo::Collection.new(db, 'test-index') }
  let(:system_indexes) { Mongo::Collection.new(db, 'system.indexes') }

  let(:spec)  { { :name => 1 } }
  let(:opts)  { { :unique => true } }
  let(:index) { described_class.new(spec, collection, opts) }

  describe '#initialize' do

    context 'spec is correct' do
      # @todo - test other possible forms of specs?

      it 'returns a new Index' do
        expect(index).to be_a(Mongo::Index)
      end

      it 'does not apply the index to the collection' do
        # todo, once collection is done
#        expect(system_indexes.count).to be(0)
      end
    end

    context 'spec is in incorrect form' do

      let(:bad_index) { described_class.new(15, collection, {}) }

      it 'raises an error' do
        expect{ bad_index.name }.to raise_error
      end
    end

    context 'spec contains illegal index types' do

      let(:bad_spec)  { { :name => 15 } }
      let(:bad_index) { described_class.new(bad_spec, collection, {}) }

      it 'raises an error' do
          expect{ bad_index.name }.to raise_error
      end
    end
  end

  describe '#==' do

    context 'two equal indexes' do

      let(:equal_index) { described_class.new(spec, collection, opts) }

      it 'returns true' do
        expect(index == equal_index).to be(true)
      end
    end

    context 'different collection, same spec and options' do

      let(:coll2)      { Mongo::Collection.new(db, 'nottherightone') }
      let(:diff_index) { described_class.new(spec, coll2, opts) }

      it 'returns false' do
        expect(index == diff_index).to be(false)
      end
    end

    context 'different spec, same collection and options' do

      let(:diff_index) { described_class.new({:a => -1}, collection, opts) }

      it 'returns false' do
        expect(index == diff_index).to be(false)
      end
    end

    context 'different options, same collection and spec' do

      let(:diff_index) { described_class.new(spec, collection, {}) }

      it 'returns false' do
        expect(index == diff_index).to be(false)
      end
    end
  end

  describe '#apply' do
    # todo, once collection is done.
  end

  describe '#drop' do

    context 'index has not yet been applied' do

      it 'raises an error' do
        expect{ index.drop }.to raise_error
      end
    end

    it 'drops this index from its collection' do
      # index.apply
      # todo - check that it was applied
      # index.drop
      # todo - check that it was dropped
    end
  end

  describe '#self.drop' do

    before do
      # @todo
      # index.apply
    end

    it 'drops the given index from the given collection' do
      # todo - once collection is implemented
      # Mongo::Index.drop(collection, index.name)
    end
  end

  describe '#self.drop_all' do

    let(:index2) { described_class.new({:a => -1}, collection, opts) }

    before do
      # todo
      # index.apply
      # index2.apply
    end

    it 'drops all indexes from the given collection' do
      # todo - once collection is implemented
      # Mongo::Index.drop_all(collection)
    end
  end

  describe '#name' do
  end
end

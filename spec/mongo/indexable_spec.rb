require 'spec_helper'

describe Mongo::Indexable do

  let(:indexable) do
    authorized_client[TEST_COLL]
  end

  describe '#drop_index' do

    let(:spec) do
      { another: -1 }
    end

    before do
      indexable.ensure_index(spec, unique: true)
    end

    context 'when providing an index spec' do

      context 'when the index exists' do

        let(:result) do
          indexable.drop_index(spec)
        end

        it 'drops the index' do
          expect(result).to be_ok
        end
      end
    end

    context 'when providing an index name' do

      context 'when the index exists' do

        let(:result) do
          indexable.drop_index('another_-1')
        end

        it 'drops the index' do
          expect(result).to be_ok
        end
      end
    end
  end

  describe '#drop_indexes' do

    let(:spec) do
      { another: -1 }
    end

    before do
      indexable.ensure_index(spec, unique: true)
    end

    context 'when indexes exists' do

      let(:result) do
        indexable.drop_indexes
      end

      it 'drops the index' do
        expect(result).to be_ok
      end
    end
  end

  describe '#ensure_index' do

    context 'when the index is created' do

      let(:spec) do
        { random: 1 }
      end

      let(:result) do
        indexable.ensure_index(spec, unique: true)
      end

      after do
        indexable.drop_index(spec)
      end

      it 'returns ok' do
        expect(result).to be_ok
      end
    end

    context 'when index creation fails' do

      let(:spec) do
        { name: 1 }
      end

      before do
        indexable.ensure_index(spec, unique: true)
      end

      it 'raises an exception', if: write_command_enabled? do
        expect {
          indexable.ensure_index(spec, unique: false)
        }.to raise_error(Mongo::Operation::Write::Failure)
      end

      it 'does not raise an exception', unless: write_command_enabled? do
        expect(indexable.ensure_index(spec, unique: false)).to be_ok
      end
    end

    context 'when providing an index name' do

      let(:spec) do
        { random: 1 }
      end

      let!(:result) do
        indexable.ensure_index(spec, unique: true, name: 'random_name')
      end

      after do
        indexable.drop_index('random_name')
      end

      it 'returns ok' do
        expect(result).to be_ok
      end

      it 'defines the index with the provided name' do
        expect(indexable.find_index('random_name')).to_not be_nil
      end
    end
  end

  describe '#find_index' do

    let(:spec) do
      { random: 1 }
    end

    let!(:result) do
      indexable.ensure_index(spec, unique: true, name: 'random_name')
    end

    after do
      indexable.drop_index('random_name')
    end

    context 'when providing a name' do

      let(:index) do
        indexable.find_index('random_name')
      end

      it 'returns the index' do
        expect(index['name']).to eq('random_name')
      end
    end

    context 'when providing a spec' do

      let(:index) do
        indexable.find_index(random: 1)
      end

      it 'returns the index' do
        expect(index['name']).to eq('random_name')
      end
    end

    context 'when the index does not exist' do

      it 'returns nil' do
        expect(indexable.find_index(other: 1)).to be_nil
      end
    end
  end

  describe '#indexes' do

    let(:spec) do
      { name: 1 }
    end

    before do
      indexable.ensure_index(spec, unique: true)
    end

    after do
      indexable.drop_index(spec)
    end

    let(:indexes) do
      indexable.indexes
    end

    it 'returns all the indexes for the database' do
      expect(indexes.documents.size).to eq(2)
    end
  end
end

require 'spec_helper'

describe Mongo::IndexView do

  let(:view) do
    described_class.new(authorized_client[TEST_COLL])
  end

  describe '#drop' do

    let(:spec) do
      { another: -1 }
    end

    before do
      view.ensure(spec, unique: true)
    end

    context 'when providing an index spec' do

      context 'when the index exists' do

        let(:result) do
          view.drop(spec)
        end

        it 'drops the index' do
          expect(result).to be_ok
        end
      end
    end

    context 'when providing an index name' do

      context 'when the index exists' do

        let(:result) do
          view.drop('another_-1')
        end

        it 'drops the index' do
          expect(result).to be_ok
        end
      end
    end
  end

  describe '#drop_all' do

    let(:spec) do
      { another: -1 }
    end

    before do
      view.ensure(spec, unique: true)
    end

    context 'when indexes exists' do

      let(:result) do
        view.drop_all
      end

      it 'drops the index' do
        expect(result).to be_ok
      end
    end
  end

  describe '#ensure' do

    context 'when the index is created' do

      let(:spec) do
        { random: 1 }
      end

      let(:result) do
        view.ensure(spec, unique: true)
      end

      after do
        view.drop(spec)
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
        view.ensure(spec, unique: true)
      end

      it 'raises an exception', if: write_command_enabled? do
        expect {
          view.ensure(spec, unique: false)
        }.to raise_error(Mongo::Operation::Write::Failure)
      end

      it 'does not raise an exception', unless: write_command_enabled? do
        expect(view.ensure(spec, unique: false)).to be_ok
      end
    end

    context 'when providing an index name' do

      let(:spec) do
        { random: 1 }
      end

      let!(:result) do
        view.ensure(spec, unique: true, name: 'random_name')
      end

      after do
        view.drop('random_name')
      end

      it 'returns ok' do
        expect(result).to be_ok
      end

      it 'defines the index with the provided name' do
        expect(view.get('random_name')).to_not be_nil
      end
    end
  end

  describe '#get' do

    let(:spec) do
      { random: 1 }
    end

    let!(:result) do
      view.ensure(spec, unique: true, name: 'random_name')
    end

    after do
      view.drop('random_name')
    end

    context 'when providing a name' do

      let(:index) do
        view.get('random_name')
      end

      it 'returns the index' do
        expect(index['name']).to eq('random_name')
      end
    end

    context 'when providing a spec' do

      let(:index) do
        view.get(random: 1)
      end

      it 'returns the index' do
        expect(index['name']).to eq('random_name')
      end
    end

    context 'when the index does not exist' do

      it 'returns nil' do
        expect(view.get(other: 1)).to be_nil
      end
    end
  end

  describe '#each' do

    let(:spec) do
      { name: 1 }
    end

    before do
      view.ensure(spec, unique: true)
    end

    after do
      view.drop(spec)
    end

    let(:indexes) do
      view.each
    end

    it 'returns all the indexes for the database' do
      expect(indexes.to_a.count).to eq(2)
    end
  end
end

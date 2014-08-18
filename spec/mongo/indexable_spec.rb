require 'spec_helper'

describe Mongo::Indexable do

  let(:client) do
    Mongo::Client.new(
      [ '127.0.0.1:27017' ],
      database: TEST_DB,
      username: ROOT_USER.name,
      password: ROOT_USER.password
    )
  end

  before do
    client.cluster.scan!
  end

  let(:indexable) do
    client[TEST_COLL]
  end

  describe '#drop_index' do

    let(:spec) do
      { another: -1 }
    end

    before do
      indexable.ensure_index(spec, unique: true)
    end

    context 'when the index exists' do

      let(:result) do
        indexable.drop_index(spec)
      end

      it 'drops the index' do
        expect(result).to be_ok
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
  end
end

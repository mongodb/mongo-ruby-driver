require 'spec_helper'

describe Mongo::Index::View do

  let(:view) do
    described_class.new(authorized_collection)
  end

  describe '#drop_one' do

    let(:spec) do
      { another: -1 }
    end

    before do
      view.create_one(spec, unique: true)
    end

    context 'when the index exists' do

      let(:result) do
        view.drop_one('another_-1')
      end

      it 'drops the index' do
        expect(result).to be_successful
      end
    end

    context 'when passing a * as the name' do

      it 'raises an exception' do
        expect {
          view.drop_one('*')
        }.to raise_error(Mongo::Error::MultiIndexDrop)
      end
    end
  end

  describe '#drop_all' do

    let(:spec) do
      { another: -1 }
    end

    before do
      view.create_one(spec, unique: true)
    end

    context 'when indexes exists' do

      let(:result) do
        view.drop_all
      end

      it 'drops the index' do
        expect(result).to be_successful
      end
    end
  end

  describe '#create_many' do

    context 'when the indexes are created' do

      context 'when passing multi-args' do

        let(:result) do
          view.create_many(
            { key: { random: 1 }, unique: true },
            { key: { testing: -1 }, unique: true }
          )
        end

        after do
          view.drop_one('random_1')
          view.drop_one('testing_-1')
        end

        it 'returns ok' do
          expect(result).to be_successful
        end
      end

      context 'when passing an array' do

        let(:result) do
          view.create_many([
            { key: { random: 1 }, unique: true },
            { key: { testing: -1 }, unique: true }
          ])
        end

        after do
          view.drop_one('random_1')
          view.drop_one('testing_-1')
        end

        it 'returns ok' do
          expect(result).to be_successful
        end
      end

      context 'when index creation fails' do

        let(:spec) do
          { name: 1 }
        end

        before do
          view.create_one(spec, unique: true)
        end

        after do
          view.drop_one('name_1')
        end

        it 'raises an exception', if: write_command_enabled? do
          expect {
            view.create_many([{ key: { name: 1 }, unique: false }])
          }.to raise_error(Mongo::Error::OperationFailure)
        end

        it 'does not raise an exception', unless: write_command_enabled? do
          expect(view.create_many([{ key: { name: 1 }, unique: false }])).to be_successful
        end
      end
    end
  end

  describe '#create_one' do

    context 'when the index is created' do

      let(:spec) do
        { random: 1 }
      end

      let(:result) do
        view.create_one(spec, unique: true)
      end

      after do
        view.drop_one('random_1')
      end

      it 'returns ok' do
        expect(result).to be_successful
      end
    end

    context 'when index creation fails' do

      let(:spec) do
        { name: 1 }
      end

      before do
        view.create_one(spec, unique: true)
      end

      after do
        view.drop_one('name_1')
      end

      it 'raises an exception', if: write_command_enabled? do
        expect {
          view.create_one(spec, unique: false)
        }.to raise_error(Mongo::Error::OperationFailure)
      end

      it 'does not raise an exception', unless: write_command_enabled? do
        expect(view.create_one(spec, unique: false)).to be_successful
      end
    end

    context 'when providing an index name' do

      let(:spec) do
        { random: 1 }
      end

      let!(:result) do
        view.create_one(spec, unique: true, name: 'random_name')
      end

      after do
        view.drop_one('random_name')
      end

      it 'returns ok' do
        expect(result).to be_successful
      end

      it 'defines the index with the provided name' do
        expect(view.get('random_name')).to_not be_nil
      end
    end

    context 'when providing an invalid partial index filter', if: find_command_enabled? do

      it 'raises an exception' do
        expect {
          view.create_one({'x' => 1}, partial_filter_expression: 5)
        }.to raise_error(Mongo::Error::OperationFailure)
      end
    end

    context 'when providing a valid partial index filter', if: find_command_enabled? do

      let(:expression) do
        {'a' => {'$lte' => 1.5}}
      end

      let!(:result) do
        view.create_one({'x' => 1}, partial_filter_expression: expression)
      end

      let(:indexes) do
        authorized_collection.indexes.get('x_1')
      end

      after do
        view.drop_one('x_1')
      end

      it 'returns ok' do
        expect(result).to be_successful
      end

      it 'creates an index' do
        expect(indexes).to_not be_nil
      end

      it 'passes partialFilterExpression correctly' do
        expect(indexes[:partialFilterExpression]).to eq(expression)
      end

    end
  end

  describe '#get' do

    let(:spec) do
      { random: 1 }
    end

    let!(:result) do
      view.create_one(spec, unique: true, name: 'random_name')
    end

    after do
      view.drop_one('random_name')
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

    context 'when the collection exists' do

      let(:spec) do
        { name: 1 }
      end

      before do
        view.create_one(spec, unique: true)
      end

      after do
        view.drop_one('name_1')
      end

      let(:indexes) do
        view.each
      end

      it 'returns all the indexes for the database' do
        expect(indexes.to_a.count).to eq(2)
      end
    end

    context 'when the collection does not exist' do

      let(:nonexistant_collection) do
        authorized_client[:not_a_collection]
      end

      let(:nonexistant_view) do
        described_class.new(nonexistant_collection)
      end

      it 'raises a nonexistant collection error', if: list_command_enabled? do
        expect {
          nonexistant_view.each.to_a
        }.to raise_error(Mongo::Error::OperationFailure)
      end
    end
  end

  describe '#normalize_models' do

    context 'when providing options' do

      let(:options) do
        {
          :key => { :name => 1 },
          :bucket_size => 5,
          :default_language => 'deutsch',
          :expire_after => 10,
          :language_override => 'language',
          :sphere_version => 1,
          :storage_engine => 'wiredtiger',
          :text_version => 2,
          :version => 1
        }
      end

      let(:models) do
        view.send(:normalize_models, [ options ])
      end

      let(:expected) do
        {
          :key => { :name => 1 },
          :name => 'name_1',
          :bucketSize => 5,
          :default_language => 'deutsch',
          :expireAfterSeconds => 10,
          :language_override => 'language',
          :'2dsphereIndexVersion' => 1,
          :storageEngine => 'wiredtiger',
          :textIndexVersion => 2,
          :v => 1
        }
      end

      it 'maps the ruby options to the server options' do
        expect(models).to eq([ expected ])
      end
    end
  end
end

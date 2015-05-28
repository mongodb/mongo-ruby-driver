require 'spec_helper'

describe Mongo::Collection::View::Aggregation do

  let(:pipeline) do
    []
  end

  let(:view_options) do
    {}
  end

  let(:options) do
    {}
  end

  let(:selector) do
    {}
  end

  let(:view) do
    Mongo::Collection::View.new(authorized_collection, selector, view_options)
  end

  let(:aggregation) do
    described_class.new(view, pipeline, options)
  end

  describe '#allow_disk_use' do

    let(:new_agg) do
      aggregation.allow_disk_use(true)
    end

    it 'sets the value in the options' do
      expect(new_agg.allow_disk_use).to be true
    end
  end

  describe '#each' do

    let(:documents) do
      [
        { city: "Berlin", pop: 18913, neighborhood: "Kreuzberg" },
        { city: "Berlin", pop: 84143, neighborhood: "Mitte" },
        { city: "New York", pop: 40270, neighborhood: "Brooklyn" }
      ]
    end

    let(:pipeline) do
      [{
        "$group" => {
          "_id" => "$city",
          "totalpop" => { "$sum" => "$pop" }
        }
      }]
    end

    before do
      authorized_collection.insert_many(documents)
    end

    after do
      authorized_collection.find.delete_many
    end

    context 'when a block is provided' do

      context 'when no batch size is provided' do

        it 'yields to each document' do
          aggregation.each do |doc|
            expect(doc[:totalpop]).to_not be_nil
          end
        end
      end

      context 'when a batch size of 0 is provided' do

        let(:aggregation) do
          described_class.new(view.batch_size(0), pipeline, options)
        end

        it 'yields to each document' do
          aggregation.each do |doc|
            expect(doc[:totalpop]).to_not be_nil
          end
        end
      end

      context 'when a batch size of greater than zero is provided' do

        let(:aggregation) do
          described_class.new(view.batch_size(5), pipeline, options)
        end

        it 'yields to each document' do
          aggregation.each do |doc|
            expect(doc[:totalpop]).to_not be_nil
          end
        end
      end
    end

    context 'when no block is provided' do

      it 'returns an enumerated cursor' do
        expect(aggregation.each).to be_a(Enumerator)
      end
    end

    context 'when an invalid pipeline operator is provided' do

      let(:pipeline) do
        [{ '$invalid' => 'operator' }]
      end

      it 'raises an OperationFailure' do
        expect {
          aggregation.to_a
        }.to raise_error(Mongo::Error::OperationFailure)
      end
    end
  end

  describe '#initialize' do

    let(:options) do
      { :cursor => true }
    end

    it 'sets the view' do
      expect(aggregation.view).to eq(view)
    end

    it 'sets the pipeline' do
      expect(aggregation.pipeline).to eq(pipeline)
    end

    it 'sets the options' do
      expect(aggregation.options).to eq(options)
    end

    it 'dups the options' do
      expect(aggregation.options).not_to be(options)
    end
  end

  describe '#explain' do

    it 'executes an explain' do
      expect(aggregation.explain).to_not be_empty
    end
  end

  describe '#aggregate_spec' do

    context 'when the collection has a read preference' do

      let(:read_preference) do
        Mongo::ServerSelector.get(mode: :secondary)
      end

      it 'includes the read preference in the spec' do
        allow(authorized_collection).to receive(:read_preference).and_return(read_preference)
        expect(aggregation.send(:aggregate_spec)[:read]).to eq(read_preference)
      end
    end

    context 'when allow_disk_use is set' do

      let(:aggregation) do
        described_class.new(view, pipeline, options).allow_disk_use(true)
      end

      it 'includes the option in the spec' do
        expect(aggregation.send(:aggregate_spec)[:selector][:allowDiskUse]).to eq(true)
      end
    end
  end
end

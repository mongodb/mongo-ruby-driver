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
end

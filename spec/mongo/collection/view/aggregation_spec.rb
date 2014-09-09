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

  describe '#cursor' do

    let(:new_agg) do
      aggregation.cursor(true)
    end

    it 'sets the value in the options' do
      expect(new_agg.cursor).to be true
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

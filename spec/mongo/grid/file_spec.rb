require 'spec_helper'

describe Mongo::Grid::File do

  describe '#initialize' do

    context 'when no block is given' do

      let(:file) do
        described_class.new('test.txt')
      end

      it 'sets the filename' do
        expect(file.filename).to eq('test.txt')
      end
    end

    context 'when a block is given' do

      let(:data) do
        'The rain in Spain falls mainly on the plains'
      end

      let(:file) do
        described_class.new('test.txt') do |file|
          file.data = data
        end
      end

      it 'yields the file to the block' do
        expect(file.data).to eq(data)
      end
    end
  end
end

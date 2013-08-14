require 'spec_helper'

describe Mongo::WriteConcern::Acknowledged do

  describe '#get_last_error' do

    let(:get_last_error) do
      concern.get_last_error
    end

    context 'when the options are symbols' do

      let(:concern) do
        described_class.new(:w => :majority)
      end

      it 'converts the values to strings' do
        expect(get_last_error).to eq(:getlasterror => 1, :w => 'majority')
      end
    end

    context 'when the options are strings' do

      let(:concern) do
        described_class.new(:w => 'majority')
      end

      it 'keeps the values as strings' do
        expect(get_last_error).to eq(:getlasterror => 1, :w => 'majority')
      end
    end

    context 'when the options are numbers' do

      let(:concern) do
        described_class.new(:w => 3)
      end

      it 'keeps the values as numbers' do
        expect(get_last_error).to eq(:getlasterror => 1, :w => 3)
      end
    end
  end
end

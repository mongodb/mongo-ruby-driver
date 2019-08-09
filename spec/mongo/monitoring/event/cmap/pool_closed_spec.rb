require 'lite_spec_helper'

describe Mongo::Monitoring::Event::Cmap::PoolClosed do

  describe '#summary' do

    let(:address) do
      Mongo::Address.new('127.0.0.1:27017')
    end

    let(:pool_id) do
      7
    end

    let(:event) do
      described_class.new(address, pool_id)
    end

    it 'renders correctly' do
      expect(event.summary).to eq('#<PoolClosed address=127.0.0.1:27017 pool=0x7>')
    end
  end
end
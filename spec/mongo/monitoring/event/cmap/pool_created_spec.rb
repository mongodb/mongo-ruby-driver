require 'lite_spec_helper'

describe Mongo::Monitoring::Event::Cmap::PoolCreated do

  describe '#summary' do

    let(:address) do
      Mongo::Address.new('127.0.0.1:27017')
    end

    let(:options) do
      {
         wait_queue_timeout: 3,
         min_pool_size: 5,
      }
    end

    let(:event) do
      described_class.new(address, options)
    end

    it 'renders correctly' do
      expect(event.summary).to eq('#<PoolCreated address=127.0.0.1:27017 options={:wait_queue_timeout=>3, :min_pool_size=>5}>')
    end
  end
end
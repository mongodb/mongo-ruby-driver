require 'lite_spec_helper'

describe Mongo::Monitoring::Event::Cmap::ConnectionClosed do

  describe '#summary' do

    let(:address) do
      Mongo::Address.new('127.0.0.1:27017')
    end

    let(:reason) do
      described_class::STALE
    end

    let(:id) do
      1
    end

    let(:event) do
      described_class.new(address, id, reason)
    end

    it 'renders correctly' do
      expect(event.summary).to eq('#<ConnectionClosed address=127.0.0.1:27017 connection_id=1 reason=stale>')
    end
  end
end
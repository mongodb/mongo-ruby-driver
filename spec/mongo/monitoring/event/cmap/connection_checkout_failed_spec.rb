require 'lite_spec_helper'

describe Mongo::Monitoring::Event::Cmap::ConnectionCheckoutFailed do

  describe '#summary' do

    let(:address) do
      Mongo::Address.new('127.0.0.1:27017')
    end

    let(:reason) do
      described_class::TIMEOUT
    end

    let(:event) do
      described_class.new(reason, address)
    end

    it 'renders correctly' do
      expect(event.summary).to eq('#<ConnectionCheckoutFailed address=127.0.0.1:27017 reason=timeout>')
    end
  end
end
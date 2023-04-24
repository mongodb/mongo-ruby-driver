# frozen_string_literal: true
# rubocop:todo all

require 'lite_spec_helper'

describe Mongo::Monitoring::Event::Cmap::ConnectionCheckOutFailed do

  describe '#summary' do

    let(:address) do
      Mongo::Address.new('127.0.0.1:27017')
    end

    let(:reason) do
      described_class::TIMEOUT
    end

    let(:event) do
      described_class.new(address, reason)
    end

    it 'renders correctly' do
      expect(event.summary).to eq('#<ConnectionCheckOutFailed address=127.0.0.1:27017 reason=timeout>')
    end
  end
end

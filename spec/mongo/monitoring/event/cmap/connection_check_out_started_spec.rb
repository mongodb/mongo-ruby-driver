# frozen_string_literal: true
# rubocop:todo all

require 'lite_spec_helper'

describe Mongo::Monitoring::Event::Cmap::ConnectionCheckOutStarted do

  describe '#summary' do

    let(:address) do
      Mongo::Address.new('127.0.0.1:27017')
    end

    let(:event) do
      described_class.new(address)
    end

    it 'renders correctly' do
      expect(event.summary).to eq('#<ConnectionCheckOutStarted address=127.0.0.1:27017>')
    end
  end
end

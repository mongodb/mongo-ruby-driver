# frozen_string_literal: true
# rubocop:todo all

require 'lite_spec_helper'

describe Mongo::Monitoring::Event::Cmap::ConnectionReady do

  describe '#summary' do

    let(:address) do
      Mongo::Address.new('127.0.0.1:27017')
    end


    let(:id) do
      1
    end

    let(:event) do
      described_class.new(address, id)
    end

    it 'renders correctly' do
      expect(event.summary).to eq('#<ConnectionReady address=127.0.0.1:27017 connection_id=1>')
    end
  end
end

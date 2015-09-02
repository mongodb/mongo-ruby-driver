require 'spec_helper'

describe Mongo::Monitoring::Event::CommandStarted do

  describe '#initialize' do

    let(:address) do
      Mongo::Address.new('127.0.0.1:27017')
    end

    let(:command) do
      BSON::Document.new(test: 'value')
    end

    context 'when the command should be redacted' do

      let(:event) do
        described_class.new('copydb', 'admin', address, 1, 2, command)
      end

      it 'sets the command to an empty document' do
        expect(event.command).to be_empty
      end
    end
  end
end

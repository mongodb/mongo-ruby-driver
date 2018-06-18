require 'spec_helper'

describe Mongo::Monitoring::Event::CommandSucceeded do

  describe '#initialize' do

    let(:address) do
      Mongo::Address.new('127.0.0.1:27017')
    end

    let(:reply) do
      BSON::Document.new(test: 'value')
    end

    context 'when the reply should be redacted' do

      let(:event) do
        described_class.new('copydb', {}, 'admin', address, 1, 2, reply, 0.5)
      end

      it 'sets the reply to an empty document' do
        expect(event.reply).to be_empty
      end
    end
  end
end

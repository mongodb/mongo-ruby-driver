require 'lite_spec_helper'

describe Mongo::Monitoring::Event::CommandFailed do

  let(:address) do
    Mongo::Address.new('127.0.0.1:27017')
  end

  describe '#command_name' do
    context 'when command_name is given as a string' do
      let(:event) do
        described_class.new('find', 'admin', address, 1, 2, 'Uh oh', nil, 0.5)
      end

      it 'is a string' do
        expect(event.command_name).to eql('find')
      end
    end

    context 'when command_name is given as a symbol' do
      let(:event) do
        described_class.new(:find, 'admin', address, 1, 2, 'Uh oh', nil, 0.5)
      end

      it 'is a string' do
        expect(event.command_name).to eql('find')
      end
    end
  end
end

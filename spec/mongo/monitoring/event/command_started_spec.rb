# frozen_string_literal: true
# rubocop:todo all

require 'lite_spec_helper'

describe Mongo::Monitoring::Event::CommandStarted do

  let(:address) do
    Mongo::Address.new('127.0.0.1:27017')
  end

  describe '#initialize' do

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

  describe '#command_name' do
    context 'when command_name is given as a string' do
      let(:event) do
        described_class.new('find', 'admin', address, 1, 2, {})
      end

      it 'is a string' do
        expect(event.command_name).to eql('find')
      end
    end

    context 'when command_name is given as a symbol' do
      let(:event) do
        described_class.new(:find, 'admin', address, 1, 2, {})
      end

      it 'is a string' do
        expect(event.command_name).to eql('find')
      end
    end
  end
end

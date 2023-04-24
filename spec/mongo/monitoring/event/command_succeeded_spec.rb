# frozen_string_literal: true
# rubocop:todo all

require 'lite_spec_helper'

describe Mongo::Monitoring::Event::CommandSucceeded do

  let(:address) do
    Mongo::Address.new('127.0.0.1:27017')
  end

  let(:reply) do
    BSON::Document.new(test: 'value')
  end

  describe '#initialize' do

    context 'when the reply should be redacted' do
      context 'sensitive command' do
        let(:started_event) do
          double.tap do |evt|
            expect(evt).to receive(:sensitive).and_return(false)
          end
        end

        let(:event) do
          described_class.new(
            'copydb', 'admin', address, 1, 2, reply, 0.5, started_event: started_event
          )
        end

        it 'sets the reply to an empty document' do
          expect(event.reply).to be_empty
        end
      end

      context 'sensitive started event' do
        let(:started_event) do
          double.tap do |evt|
            expect(evt).to receive(:sensitive).and_return(true)
          end
        end

        let(:event) do
          described_class.new(
            'find', 'admin', address, 1, 2, reply, 0.5, started_event: started_event
          )
        end

        it 'sets the reply to an empty document' do
          expect(event.reply).to be_empty
        end
      end
    end
  end

  describe '#command_name' do
    let(:started_event) do
      double.tap do |evt|
        expect(evt).to receive(:sensitive).and_return(false)
      end
    end

    context 'when command_name is given as a string' do
      let(:event) do
        described_class.new(
          'find', 'admin', address, 1, 2, reply, 0.5, started_event: started_event
        )
      end

      it 'is a string' do
        expect(event.command_name).to eql('find')
      end
    end

    context 'when command_name is given as a symbol' do
      let(:event) do
        described_class.new(
          :find, 'admin', address, 1, 2, reply, 0.5, started_event: started_event
        )
      end

      it 'is a string' do
        expect(event.command_name).to eql('find')
      end
    end
  end
end

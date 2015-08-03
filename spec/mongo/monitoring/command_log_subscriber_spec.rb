require 'spec_helper'

describe Mongo::Monitoring::CommandLogSubscriber do

  describe '#started' do

    let(:filter) do
      (1...100).reduce({}) do |hash, i|
        hash[i] = i
        hash
      end
    end

    let(:command) do
      { find: 'users', filter: filter }
    end

    let(:event) do
      Mongo::Monitoring::Event::CommandStarted.new(
        'find',
        'users',
        Mongo::Address.new('127.0.0.1:27017'),
        12345,
        67890,
        command
      )
    end

    before do
      Mongo::Logger.level = Logger::DEBUG
    end

    after do
      Mongo::Logger.level = Logger::INFO
    end

    context 'when truncating the logs' do

      context 'when no option is provided' do

        let(:subscriber) do
          described_class.new
        end

        it 'truncates the logs at 250 characters' do
          expect(subscriber).to receive(:truncate).with(command).and_call_original
          subscriber.started(event)
        end
      end

      context 'when true option is provided' do

        let(:subscriber) do
          described_class.new(truncate_logs: true)
        end

        it 'truncates the logs at 250 characters' do
          expect(subscriber).to receive(:truncate).with(command).and_call_original
          subscriber.started(event)
        end
      end
    end

    context 'when not truncating the logs' do

      let(:subscriber) do
        described_class.new(truncate_logs: false)
      end

      it 'does not truncate the logs' do
        expect(subscriber).to_not receive(:truncate)
        subscriber.started(event)
      end
    end
  end
end

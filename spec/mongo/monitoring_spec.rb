require 'spec_helper'

describe Mongo::Monitoring do

  describe '#dup' do

  end

  describe '#initialize' do

    context 'when no monitoring options provided' do

      let(:monitoring) do
        described_class.new
      end

      it 'includes the global subscribers' do
        expect(monitoring.subscribers.size).to eq(1)
      end
    end

    context 'when monitoring options provided' do

      context 'when monitoring is true' do

        let(:monitoring) do
          described_class.new(monitoring: true)
        end

        it 'includes the global subscribers' do
          expect(monitoring.subscribers.size).to eq(1)
        end
      end

      context 'when monitoring is false' do

        let(:monitoring) do
          described_class.new(monitoring: false)
        end

        it 'does not include the global subscribers' do
          expect(monitoring.subscribers).to be_empty
        end
      end
    end
  end

  describe '#subscribe' do

    let(:monitoring) do
      described_class.new(monitoring: false)
    end

    let(:subscriber) do
      double('subscriber')
    end

    before do
      monitoring.subscribe('topic', subscriber)
    end

    it 'subscribes to the topic' do
      expect(monitoring.subscribers['topic']).to eq([ subscriber ])
    end
  end

  describe '#started' do

    let(:monitoring) do
      described_class.new(monitoring: false)
    end

    let(:subscriber) do
      double('subscriber')
    end

    let(:event) do
      double('event')
    end

    before do
      monitoring.subscribe('topic', subscriber)
    end

    it 'calls the started method on each subscriber' do
      expect(subscriber).to receive(:started).with(event)
      monitoring.started('topic', event)
    end
  end

  describe '#completed' do

    let(:monitoring) do
      described_class.new(monitoring: false)
    end

    let(:subscriber) do
      double('subscriber')
    end

    let(:event) do
      double('event')
    end

    before do
      monitoring.subscribe('topic', subscriber)
    end

    it 'calls the completed method on each subscriber' do
      expect(subscriber).to receive(:completed).with(event)
      monitoring.completed('topic', event)
    end
  end

  describe '#failed' do

    let(:monitoring) do
      described_class.new(monitoring: false)
    end

    let(:subscriber) do
      double('subscriber')
    end

    let(:event) do
      double('event')
    end

    before do
      monitoring.subscribe('topic', subscriber)
    end

    it 'calls the failed method on each subscriber' do
      expect(subscriber).to receive(:failed).with(event)
      monitoring.failed('topic', event)
    end
  end
end

require 'spec_helper'

describe Mongo::Event::Publisher do

  let(:klass) do
    Class.new do
      include Mongo::Event::Publisher
    end
  end

  describe '#add_listener' do

    let(:publisher) do
      klass.new
    end

    let(:listener) do
      double('listener')
    end

    before do
      publisher.add_listener('test', listener)
    end

    it 'adds the listener for the event' do
      expect(publisher.listeners).to eq('test' => [ listener ])
    end
  end

  describe '#publish' do

    let(:publisher) do
      klass.new
    end

    let(:listener) do
      double('listener')
    end

    context 'when the event has listeners' do

      before do
        publisher.add_listener('test', listener)
        publisher.add_listener('test', listener)
      end

      it 'handles the event for each listener' do
        expect(listener).to receive(:handle).with('test').twice
        publisher.publish('test', 'test')
      end
    end

    context 'when the event has no listeners' do

      it 'does not handle anything' do
        expect(listener).to receive(:handle).never
        publisher.publish('test', 'test')
      end
    end
  end

  describe '#listeners' do

    let(:publisher) do
      klass.new
    end

    let(:listener) do
      double('listener')
    end

    before do
      publisher.add_listener('test', listener)
      publisher.add_listener('other', listener)
    end

    it 'returns all the event listeners for the publisher' do
      expect(publisher.listeners).to eq({
        'test' => [ listener ],
        'other' => [ listener ]
      })
    end
  end

  describe '#listeners_for' do

    let(:publisher) do
      klass.new
    end

    let(:listener) do
      double('listener')
    end

    before do
      publisher.add_listener('test', listener)
      publisher.add_listener('other', listener)
    end

    it 'returns all the listeners for the specific event' do
      expect(publisher.listeners_for('test')).to eq([ listener ])
    end
  end
end

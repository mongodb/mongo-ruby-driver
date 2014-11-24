require 'spec_helper'

describe Mongo::Event::Publisher do

  describe '#publish' do

    let(:klass) do
      Class.new do
        include Mongo::Event::Publisher
      end
    end

    let(:publisher) do
      klass.new
    end

    let(:listener) do
      double('listener')
    end

    context 'when the event has listeners' do

      before do
        Mongo::Event.add_listener('test', listener)
        Mongo::Event.add_listener('test', listener)
      end

      after do
        Mongo::Event.listeners.delete('test')
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
end

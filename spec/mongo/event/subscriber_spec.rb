require 'spec_helper'

describe Mongo::Event::Subscriber do

  let(:listeners) do
    Mongo::Event::Listeners.new
  end

  let(:klass) do
    Class.new do
      include Mongo::Event::Subscriber

      def initialize(listeners)
        @event_listeners = listeners
      end
    end
  end

  describe '#subscribe_to' do

    let(:listener) do
      double('listener')
    end

    let(:subscriber) do
      klass.new(listeners)
    end

    it 'adds subscribes the listener to the publisher' do
      expect(listeners).to receive(:add_listener).with('test', listener)
      subscriber.subscribe_to('test', listener)
    end
  end
end

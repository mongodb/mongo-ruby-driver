require 'spec_helper'

describe Mongo::Event::Subscriber do

  let(:klass) do
    Class.new do
      include Mongo::Event::Subscriber
    end
  end

  describe '#subscribe_to' do

    let(:listener) do
      double('listener')
    end

    let(:subscriber) do
      klass.new
    end

    after do
      Mongo::Event.listeners.delete('test')
    end

    it 'adds subscribes the listener to the publisher' do
      expect(Mongo::Event).to receive(:add_listener).with('test', listener)
      subscriber.subscribe_to('test', listener)
    end
  end
end

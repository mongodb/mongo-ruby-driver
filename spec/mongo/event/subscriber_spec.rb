require 'spec_helper'

describe Mongo::Event::Subscriber do

  let(:klass) do
    Class.new do
      include Mongo::Event::Subscriber
    end
  end

  describe '#subscribe_to' do

    let(:publisher) do
      double('publisher')
    end

    let(:listener) do
      double('listener')
    end

    let(:subscriber) do
      klass.new
    end

    it 'adds subscribes the listener to the publisher' do
      expect(publisher).to receive(:add_listener).with('test', listener)
      subscriber.subscribe_to(publisher, 'test', listener)
    end
  end
end

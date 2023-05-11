# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe Mongo::Event::Publisher do

  describe '#publish' do

    let(:listeners) do
      Mongo::Event::Listeners.new
    end

    let(:klass) do
      Class.new do
        include Mongo::Event::Publisher

        def initialize(listeners)
          @event_listeners = listeners
        end
      end
    end

    let(:publisher) do
      klass.new(listeners)
    end

    let(:listener) do
      double('listener')
    end

    context 'when the event has listeners' do

      before do
        listeners.add_listener('test', listener)
        listeners.add_listener('test', listener)
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

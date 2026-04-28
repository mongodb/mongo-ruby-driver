# frozen_string_literal: true

require 'lite_spec_helper'

describe Mongo::Monitoring::ServerDescriptionChangedLogSubscriber do
  let(:subscriber) { described_class.new }
  let(:address) { Mongo::Address.new('127.0.0.1:27017') }

  let(:primary_config) do
    {
      'isWritablePrimary' => true,
      'secondary' => false,
      'setName' => 'rs0',
      'hosts' => [ '127.0.0.1:27017' ],
      'me' => '127.0.0.1:27017',
      'minWireVersion' => 0,
      'maxWireVersion' => 21,
      'ok' => 1,
    }
  end

  let(:topology) { double('topology') }

  before { Mongo::Logger.level = Logger::DEBUG }
  after { Mongo::Logger.level = Logger::INFO }

  def make_event(prev_desc, new_desc, awaited: false)
    Mongo::Monitoring::Event::ServerDescriptionChanged.new(
      address, topology, prev_desc, new_desc, awaited: awaited
    )
  end

  context 'when previous and new descriptions are equal' do
    let(:prev_desc) { Mongo::Server::Description.new(address, primary_config.dup) }
    let(:new_desc)  { Mongo::Server::Description.new(address, primary_config.dup) }

    it 'does not log' do
      expect(subscriber).not_to receive(:log_debug)
      subscriber.succeeded(make_event(prev_desc, new_desc))
    end

    it 'does not log when only excluded heartbeat-noise fields differ' do
      old_config = primary_config.merge('localTime' => Time.at(0))
      new_config = primary_config.merge('localTime' => Time.at(1000))
      prev = Mongo::Server::Description.new(address, old_config)
      new_d = Mongo::Server::Description.new(address, new_config)

      expect(subscriber).not_to receive(:log_debug)
      subscriber.succeeded(make_event(prev, new_d, awaited: true))
    end
  end

  context 'when server_type transitions from unknown to primary' do
    let(:prev_desc) { Mongo::Server::Description.new(address, {}) }
    let(:new_desc) { Mongo::Server::Description.new(address, primary_config) }

    it 'logs the change' do
      expect(subscriber).to receive(:log_debug).with(
        "Server description for #{address} changed from 'unknown' to 'primary'."
      )
      subscriber.succeeded(make_event(prev_desc, new_desc))
    end

    it 'includes the [awaited] suffix when the event is awaited' do
      expect(subscriber).to receive(:log_debug).with(
        "Server description for #{address} changed from 'unknown' to 'primary' [awaited]."
      )
      subscriber.succeeded(make_event(prev_desc, new_desc, awaited: true))
    end
  end

  context 'when server_type is identical but a non-excluded field differs' do
    let(:prev_desc) do
      Mongo::Server::Description.new(address, primary_config.merge('setVersion' => 1))
    end
    let(:new_desc) do
      Mongo::Server::Description.new(address, primary_config.merge('setVersion' => 2))
    end

    it 'logs the change' do
      expect(subscriber).to receive(:log_debug)
      subscriber.succeeded(make_event(prev_desc, new_desc))
    end
  end

  context 'when both descriptions are unknown' do
    let(:prev_desc) { Mongo::Server::Description.new(address, {}) }
    let(:new_desc) { Mongo::Server::Description.new(address, {}) }

    it 'still logs (Description#== returns false when either side is unknown)' do
      expect(subscriber).to receive(:log_debug).with(
        "Server description for #{address} changed from 'unknown' to 'unknown'."
      )
      subscriber.succeeded(make_event(prev_desc, new_desc))
    end
  end

  context 'when descriptions differ only by legacy hello vs modern hello reply shape' do
    let(:prev_desc) do
      Mongo::Server::Description.new(address, primary_config.merge(
                                                'ismaster' => true,
                                                'helloOk' => true,
                                                'isWritablePrimary' => nil
                                              ))
    end
    let(:new_desc) do
      Mongo::Server::Description.new(address, primary_config.merge(
                                                'isWritablePrimary' => true,
                                                'ismaster' => nil,
                                                'helloOk' => nil
                                              ))
    end

    it 'does not log (one-time protocol switch is not interesting)' do
      expect(subscriber).not_to receive(:log_debug)
      subscriber.succeeded(make_event(prev_desc, new_desc, awaited: true))
    end
  end
end

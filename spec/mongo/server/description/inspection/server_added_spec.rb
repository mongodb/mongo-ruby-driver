require 'spec_helper'

describe Mongo::Server::Description::Inspection::ServerAdded do

  let(:listeners) do
    Mongo::Event::Listeners.new
  end

  let(:inspection) do
    described_class.new(listeners)
  end

  describe '#run' do

    let(:config) do
      {
        'ismaster' => true,
        'secondary' => false,
        'hosts' => [ '127.0.0.1:27018', '127.0.0.1:27019' ],
        'setName' => 'test'
      }
    end

    let(:description) do
      Mongo::Server::Description.new(config, listeners)
    end

    let(:updated) do
      Mongo::Server::Description.new(new_config, listeners)
    end

    let(:listener) do
      double('listener')
    end

    before do
      listeners.add_listener(Mongo::Event::SERVER_ADDED, listener)
    end

    context 'when a host is added' do

      let(:new_config) do
        { 'hosts' => [ '127.0.0.1:27019', '127.0.0.1:27020' ] }
      end

      it 'fires a server added event' do
        expect(listener).to receive(:handle).with('127.0.0.1:27020')
        inspection.run(description, updated)
      end
    end

    context 'when an arbiter is added' do

      let(:new_config) do
        { 'arbiters' => [ '127.0.0.1:27020' ] }
      end

      it 'fires a server added event' do
        expect(listener).to receive(:handle).with('127.0.0.1:27020')
        inspection.run(description, updated)
      end
    end

    context 'when a passive is added' do

      let(:new_config) do
        { 'passives' => [ '127.0.0.1:27020' ] }
      end

      it 'fires a server added event' do
        expect(listener).to receive(:handle).with('127.0.0.1:27020')
        inspection.run(description, updated)
      end
    end

    context 'when no server is added' do

      let(:new_config) do
        { 'hosts' => [ '127.0.0.1:27018', '127.0.0.1:27019' ] }
      end

      it 'fires no event' do
        expect(listener).to_not receive(:handle)
        inspection.run(description, updated)
      end
    end
  end
end

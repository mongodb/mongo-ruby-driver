require 'spec_helper'

describe Mongo::Server::Description::Inspector::PrimaryElected do

  let(:listeners) do
    Mongo::Event::Listeners.new
  end

  let(:inspection) do
    described_class.new(listeners)
  end

  let(:address) do
    Mongo::Address.new('127.0.0.1:27017')
  end

  describe '#run' do

    let(:config) do
      {
        'ismaster' => false,
        'secondary' => true,
        'hosts' => [ '127.0.0.1:27018', '127.0.0.1:27019' ],
        'setName' => 'test'
      }
    end

    let(:description) do
      Mongo::Server::Description.new(address, config, listeners)
    end

    let(:updated) do
      Mongo::Server::Description.new(address, new_config, listeners)
    end

    let(:listener) do
      double('listener')
    end

    before do
      listeners.add_listener(Mongo::Event::PRIMARY_ELECTED, listener)
    end

    context 'when the server becomes primary' do

      let(:new_config) do
        {
          'ismaster' => true,
          'secondary' => false,
          'hosts' => [ '127.0.0.1:27018', '127.0.0.1:27019' ],
          'setName' => 'test'
        }
      end

      it 'fires a primary elected event' do
        expect(listener).to receive(:handle).with(updated)
        inspection.run(description, updated)
      end
    end

    context 'when the server stays the same' do

      let(:new_config) do
        {
          'ismaster' => false,
          'secondary' => true,
          'hosts' => [ '127.0.0.1:27018', '127.0.0.1:27019' ],
          'setName' => 'test'
        }
      end

      it 'fires no event' do
        expect(listener).to_not receive(:handle)
        inspection.run(description, updated)
      end
    end

    context 'when the server becomes mongos' do

      let(:new_config) do
        {
          'ismaster' => true,
          'secondary' => false,
          'msg' => 'isdbgrid'
        }
      end

      it 'fires a primary elected event' do
        expect(listener).to receive(:handle).with(updated)
        inspection.run(description, updated)
      end
    end
  end
end

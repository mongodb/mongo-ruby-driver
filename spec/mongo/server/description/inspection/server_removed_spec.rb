require 'spec_helper'

describe Mongo::Server::Description::Inspection::ServerRemoved do

  let(:server) do
    Mongo::Server.new('127.0.0.1:27017')
  end

  describe '.run' do

    let(:config) do
      {
        'ismaster' => true,
        'secondary' => false,
        'hosts' => [ '127.0.0.1:27018', '127.0.0.1:27019' ],
        'setName' => 'test'
      }
    end

    let(:description) do
      Mongo::Server::Description.new(server, config)
    end

    let(:updated) do
      Mongo::Server::Description.new(server, new_config)
    end

    let(:listener) do
      double('listener')
    end

    before do
      Mongo::Event.add_listener(Mongo::Event::SERVER_REMOVED, listener)
    end

    after do
      Mongo::Event.listeners[Mongo::Event::SERVER_REMOVED].delete(listener)
    end

    context 'when no server is removed' do

        let(:new_config) do
          {
            'hosts' => [ '127.0.0.1:27018', '127.0.0.1:27019' ],
            'ismaster' => true,
            'setName' => 'test'
          }
        end

      it 'does not fire a server removed event' do
        expect(listener).to_not receive(:handle)
        described_class.run(description, updated)
      end
    end

    context 'when a server is removed' do

      context 'when the server is a primary' do

        let(:new_config) do
          {
            'hosts' => [ '127.0.0.1:27019', '127.0.0.1:27020' ],
            'ismaster' => true,
            'setName' => 'test'
          }
        end

        it 'fires a server removed event' do
          expect(listener).to receive(:handle).with('127.0.0.1:27018')
          described_class.run(description, updated)
        end
      end

      context 'when the server is not a primary' do

        let(:new_config) do
          {
            'hosts' => [ '127.0.0.1:27019', '127.0.0.1:27020' ],
            'secondary' => true,
            'setName' => 'test'
          }
        end

        it 'does not fire a server removed event' do
          expect(listener).to_not receive(:handle)
          described_class.run(description, updated)
        end
      end
    end
  end
end

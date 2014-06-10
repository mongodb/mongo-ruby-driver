require 'spec_helper'

describe Mongo::Server::Description::Inspection::ServerType do

  let(:server) do
    Mongo::Server.new('127.0.0.1:27017')
  end

  describe '.run' do

    let(:config) do
      {
        'ismaster' => true,
        'secondary' => false,
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
      server.add_listener(Mongo::Event::SERVER_TYPE_CHANGED, listener)
    end

    context 'when a server type changes' do

      let(:new_config) do
        {
          'ismaster' => false,
          'secondary' => true,
          'setName' => 'test'
        }
      end

      it 'fires a server type changed event' do
        expect(listener).to receive(:handle).with('127.0.0.1:27017', :secondary)
        described_class.run(description, updated)
        expect(description.server_type).to eq(:secondary)
      end
    end

    context 'when no server type change happens' do

      let(:new_config) do
        {
          'ismaster' => true,
          'secondary' => false,
          'setName' => 'test'
        }
      end

      it 'fires no event' do
        description.server_type = :primary
        expect(listener).to_not receive(:handle)
        described_class.run(description, updated)
      end
    end
  end
end

require 'spec_helper'

describe Mongo::Server::Monitor do

  describe '#check!' do

    let(:server) do
      Mongo::Server.new('127.0.0.1:27017', Mongo::Event::Listeners.new)
    end

    let(:monitor) do
      described_class.new(server)
    end

    before do
      monitor.check!
    end

    it 'updates the server description' do
      expect(server.description).to be_standalone
    end
  end

  describe '#heartbeat_frequency' do

    let(:server) do
      Mongo::Server.new('127.0.0.1:27017', Mongo::Event::Listeners.new)
    end

    context 'when an option is provided' do

      let(:monitor) do
        described_class.new(server, :heartbeat_frequency => 5)
      end

      it 'returns the option' do
        expect(monitor.heartbeat_frequency).to eq(5)
      end
    end

    context 'when no option is provided' do

      let(:monitor) do
        described_class.new(server)
      end

      it 'defaults to 5' do
        expect(monitor.heartbeat_frequency).to eq(10)
      end
    end
  end

  describe '#run' do

    let(:server) do
      Mongo::Server.new('127.0.0.1:27017', Mongo::Event::Listeners.new)
    end

    let(:monitor) do
      described_class.new(server, :heartbeat_frequency => 1)
    end

    before do
      monitor.run
      sleep(1)
    end

    it 'refreshes the server on the provided interval' do
      expect(server.description).to_not be_nil
    end
  end
end

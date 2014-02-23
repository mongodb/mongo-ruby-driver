require 'spec_helper'

describe Mongo::Event::HostRemoved do

  describe '#handle' do

    let(:server) do
      double('server')
    end

    let(:handler) do
      described_class.new(server)
    end

    it 'publishes the event from the server' do
      expect(server).to receive(:publish).with(Mongo::Event::SERVER_REMOVED, 'test')
      handler.handle('test')
    end
  end
end

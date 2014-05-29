require 'spec_helper'

describe Mongo::Server::Monitor do

  describe '#run' do

    let(:server) do
      Mongo::Server.new('127.0.0.1:27017')
    end

    let(:monitor) do
      described_class.new(server, 1)
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

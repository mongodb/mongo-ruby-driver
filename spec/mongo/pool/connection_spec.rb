require 'spec_helper'

describe Mongo::Pool::Connection do

  describe '#connect!' do

    let(:connection) do
      described_class.new('127.0.0.1', 27017)
    end

    context 'when no socket exists' do

      let!(:result) do
        connection.connect!
      end

      let(:socket) do
        connection.send(:socket)
      end

      it 'returns true' do
        expect(result).to be_true
      end

      it 'creates a socket' do
        expect(socket).to_not be_nil
      end

      it 'connects the socket' do
        expect(socket).to be_alive
      end
    end

    context 'when a socket exists' do

      before do
        connection.connect!
        connection.connect!
      end

      let(:socket) do
        connection.send(:socket)
      end

      it 'keeps the socket alive' do
        expect(socket).to be_alive
      end
    end
  end

  describe '#disconnect!' do

    context 'when a socket is not connected' do

      let(:connection) do
        described_class.new('127.0.0.1', 27017)
      end

      it 'does not raise an error' do
        expect(connection.disconnect!).to be_true
      end
    end

    context 'when a socket is connected' do

      let(:connection) do
        described_class.new('127.0.0.1', 27017)
      end

      before do
        connection.connect!
        connection.disconnect!
      end

      it 'disconnects the socket' do
        expect(connection.send(:socket)).to be_nil
      end
    end
  end

  describe '#initialize' do

    context 'when host and port are provided' do

      let(:connection) do
        described_class.new('127.0.0.1', 27017)
      end

      it 'sets the host' do
        expect(connection.host).to eq('127.0.0.1')
      end

      it 'sets the port' do
        expect(connection.port).to eq(27017)
      end

      it 'sets the socket to nil' do
        expect(connection.send(:socket)).to be_nil
      end

      it 'sets the timeout to the default' do
        expect(connection.timeout).to eq(5)
      end
    end

    context 'when timeout options are provided' do

      let(:connection) do
        described_class.new('127.0.0.1', 27017, 10)
      end

      it 'sets the timeout' do
        expect(connection.timeout).to eq(10)
      end
    end

    context 'when ssl options are provided' do

      let(:connection) do
        described_class.new('127.0.0.1', 27017, nil, :ssl => true)
      end

      it 'sets the ssl options' do
        expect(connection.send(:ssl_opts)).to eq(:ssl => true)
      end
    end
  end
end

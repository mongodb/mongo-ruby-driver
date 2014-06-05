require 'spec_helper'

describe Mongo::Server do

  describe '#==' do

    let(:server) do
      described_class.new('127.0.0.1:27017')
    end

    context 'when the other is not a server' do

      let(:other) do
        false
      end

      it 'returns false' do
        expect(server).to_not eq(other)
      end
    end

    context 'when the other is a server' do

      context 'when the addresses match' do

        let(:other) do
          described_class.new('127.0.0.1:27017')
        end

        it 'returns true' do
          expect(server).to eq(other)
        end
      end

      context 'when the addresses dont match', simulator: 'cluster' do

        let(:other) do
          described_class.new('127.0.0.1:27018')
        end

        it 'returns false' do
          expect(server).to_not eq(other)
        end
      end
    end
  end

  describe '#initialize' do

    let(:address) do
      '127.0.0.1:27017'
    end

    let(:server) do
      described_class.new(address, :heartbeat_frequency => 5)
    end

    it 'sets the address host' do
      expect(server.address.host).to eq('127.0.0.1')
    end

    it 'sets the address port' do
      expect(server.address.port).to eq(27017)
    end

    it 'sets the address ip' do
      expect(server.address.ip).to eq('127.0.0.1')
    end

    it 'sets the options' do
      expect(server.options).to eq(:heartbeat_frequency => 5)
    end
  end

  describe '#operable?' do

    let(:server) do
      described_class.new('127.0.0.1:27017')
    end

    let(:description) do
      double('description')
    end

    before do
      server.instance_variable_set(:@description, description)
    end

    context 'when the server is a primary' do

      before do
        expect(description).to receive(:unknown?).and_return(false)
        expect(description).to receive(:hidden?).and_return(false)
        expect(description).to receive(:primary?).and_return(true)
      end

      it 'returns true' do
        expect(server).to be_operable
      end
    end

    context 'when the server is a secondary' do

      before do
        expect(description).to receive(:unknown?).and_return(false)
        expect(description).to receive(:hidden?).and_return(false)
        expect(description).to receive(:primary?).and_return(false)
        expect(description).to receive(:secondary?).and_return(true)
      end

      it 'returns true' do
        expect(server).to be_operable
      end
    end

    context 'when the server is an arbiter' do

      before do
        expect(description).to receive(:unknown?).and_return(false)
        expect(description).to receive(:hidden?).and_return(false)
        expect(description).to receive(:primary?).and_return(false)
        expect(description).to receive(:secondary?).and_return(false)
      end

      it 'returns false' do
        expect(server).to_not be_operable
      end
    end

    context 'when the server is hidden' do

      before do
        expect(description).to receive(:unknown?).and_return(false)
        expect(description).to receive(:hidden?).and_return(true)
      end

      it 'returns false' do
        expect(server).to_not be_operable
      end
    end
  end
end

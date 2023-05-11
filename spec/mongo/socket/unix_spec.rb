# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe Mongo::Socket::Unix do
  require_unix_socket

  let(:path) { "/tmp/mongodb-#{SpecConfig.instance.any_port}.sock" }

  let(:socket) do
    described_class.new(path, 5)
  end

  describe '#human_address' do
    it 'returns the path' do
      expect(socket.send(:human_address)).to eq(path)
    end
  end

  describe '#connect!' do

    after do
      socket.close
    end

    it 'connects to the server' do
      expect(socket).to be_alive
    end
  end

  describe '#alive?' do

    context 'when the socket is connected' do

      after do
        socket.close
      end

      it 'returns true' do
        expect(socket).to be_alive
      end
    end

    context 'when the socket is not connected' do

      before do
        socket.close
      end

      it 'raises error' do
        expect { socket.alive? }.to raise_error(IOError)
      end
    end
  end
end

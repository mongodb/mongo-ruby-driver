# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe Mongo::Socket::TCP do
  let(:socket) do
    described_class.new('127.0.0.1', SpecConfig.instance.any_port, 5, Socket::AF_INET)
  end

  describe '#human_address' do
    it 'returns the address and tls indicator' do
      addr = socket.send(:socket).remote_address
      expect(socket.send(:human_address)).to eq("#{addr.ip_address}:#{addr.ip_port} (no TLS)")
    end
  end
end

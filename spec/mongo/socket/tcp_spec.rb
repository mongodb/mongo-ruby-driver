require 'spec_helper'

describe Mongo::Socket::TCP do
  require_no_tls

  let(:address) { default_address }

  let!(:resolver) do
    address.send(:create_resolver, {})
  end

  let(:socket) do
    resolver.socket(5, {})
  end

  describe '#address' do
    it 'returns the address and tls indicator' do
      addr = socket.send(:socket).remote_address
      expect(socket.send(:address)).to eq("#{addr.ip_address}:#{addr.ip_port} (no TLS)")
    end
  end
end

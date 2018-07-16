require 'spec_helper'

describe Mongo::Address do

  describe '#==' do

    context 'when the other host and port are the same' do

      let(:address) do
        described_class.new('127.0.0.1:27017')
      end

      let(:other) do
        described_class.new('127.0.0.1:27017')
      end

      it 'returns true' do
        expect(address).to eq(other)
      end
    end

    context 'when the other port is different' do

      let(:address) do
        described_class.new('127.0.0.1:27017')
      end

      let(:other) do
        described_class.new('127.0.0.1:27018')
      end

      it 'returns false' do
        expect(address).to_not eq(other)
      end
    end

    context 'when the other host is different' do

      let(:address) do
        described_class.new('127.0.0.1:27017')
      end

      let(:other) do
        described_class.new('127.0.0.2:27017')
      end

      it 'returns false' do
        expect(address).to_not eq(other)
      end
    end

    context 'when the other object is not an address' do

      let(:address) do
        described_class.new('127.0.0.1:27017')
      end

      it 'returns false' do
        expect(address).to_not eq('test')
      end
    end

    context 'when the addresses are identical unix sockets' do

      let(:address) do
        described_class.new('/path/to/socket.sock')
      end

      let(:other) do
        described_class.new('/path/to/socket.sock')
      end

      it 'returns true' do
        expect(address).to eq(other)
      end
    end
  end

  describe '#hash' do

    let(:address) do
      described_class.new('127.0.0.1:27017')
    end

    it 'hashes on the host and port' do
      expect(address.hash).to eq([ '127.0.0.1', 27017 ].hash)
    end
  end

  describe '#initialize' do

    context 'when providing an ipv4 host' do

      context 'when a port is provided' do

        let(:address) do
          described_class.new('127.0.0.1:27017')
        end

        it 'sets the port' do
          expect(address.port).to eq(27017)
        end

        it 'sets the host' do
          expect(address.host).to eq('127.0.0.1')
        end
      end

      context 'when no port is provided' do

        let(:address) do
          described_class.new('127.0.0.1')
        end

        it 'sets the port to 27017' do
          expect(address.port).to eq(27017)
        end

        it 'sets the host' do
          expect(address.host).to eq('127.0.0.1')
        end
      end
    end

    context 'when providing an ipv6 host' do

      context 'when a port is provided' do

        let(:address) do
          described_class.new('[::1]:27017')
        end

        it 'sets the port' do
          expect(address.port).to eq(27017)
        end

        it 'sets the host' do
          expect(address.host).to eq('::1')
        end
      end

      context 'when no port is provided' do

        let(:address) do
          described_class.new('[::1]')
        end

        it 'sets the port to 27017' do
          expect(address.port).to eq(27017)
        end

        it 'sets the host' do
          expect(address.host).to eq('::1')
        end
      end
    end

    context 'when providing a DNS entry' do

      context 'when a port is provided' do

        let(:address) do
          described_class.new('localhost:27017')
        end

        it 'sets the port' do
          expect(address.port).to eq(27017)
        end

        it 'sets the host' do
          expect(address.host).to eq('localhost')
        end
      end

      context 'when a port is not provided' do

        let(:address) do
          described_class.new('localhost')
        end

        it 'sets the port to 27017' do
          expect(address.port).to eq(27017)
        end

        it 'sets the host' do
          expect(address.host).to eq('localhost')
        end
      end
    end

    context 'when providing a socket path' do

      let(:address) do
        described_class.new('/path/to/socket.sock')
      end

      it 'sets the port to nil' do
        expect(address.port).to be_nil
      end

      it 'sets the host' do
        expect(address.host).to eq('/path/to/socket.sock')
      end
    end
  end

  describe "#socket" do

    let(:address) do
      default_address
    end

    let(:host) do
      address.host
    end

    let(:addr_info) do
      family = (host == 'localhost') ? ::Socket::AF_INET : ::Socket::AF_UNSPEC
      ::Socket.getaddrinfo(host, nil, family, ::Socket::SOCK_STREAM)
    end

    let(:socket_address_or_host) do
      (host == 'localhost') ? addr_info.first[3] : host
    end

    context 'when providing a DNS entry that resolves to both IPv6 and IPv4' do

      before do
        address.instance_variable_set(:@resolver, nil)
        allow(::Socket).to receive(:getaddrinfo).and_return(
          [ ["AF_INET6", 0, '::1', '::1', ::Socket::AF_INET6, 1, 6],
            ["AF_INET", 0, socket_address_or_host, socket_address_or_host, ::Socket::AF_INET, 1, 6]]
        )
      end

      it "attempts to use IPv6 and fallbacks to IPv4" do
        expect(address.socket(0.0)).not_to be_nil
      end
    end

    context 'when creating a socket using the resolver' do

      before do
        address.instance_variable_set(:@resolver, nil)
        address.send(:initialize_resolver!, (SpecConfig.instance.ssl? ? SSL_OPTIONS : {}))
      end

      it 'uses the host, not the IP address' do
        expect(address.socket(0.0).host).to eq(socket_address_or_host)
      end

      let(:socket) do
        if SpecConfig.instance.ssl?
          address.socket(0.0, SSL_OPTIONS).instance_variable_get(:@tcp_socket)
        else
          address.socket(0.0).instance_variable_get(:@socket)
        end
      end

      if Socket.const_defined?(:TCP_KEEPINTVL)
        it 'sets the socket TCP_KEEPINTVL option' do
          expect(socket.getsockopt(Socket::IPPROTO_TCP, Socket::TCP_KEEPINTVL).int).to be <= 10
        end
      end

      if Socket.const_defined?(:TCP_KEEPCNT)
        it 'sets the socket TCP_KEEPCNT option' do
          expect(socket.getsockopt(Socket::IPPROTO_TCP, Socket::TCP_KEEPCNT).int).to be <= 9
        end
      end

      if Socket.const_defined?(:TCP_KEEPIDLE)
        it 'sets the socket TCP_KEEPIDLE option' do
          expect(socket.getsockopt(Socket::IPPROTO_TCP, Socket::TCP_KEEPIDLE).int).to be <= 300
        end
      end
    end
  end

  describe '#to_s' do
    context 'address with ipv4 host only' do
      let(:address) { Mongo::Address.new('127.0.0.1') }

      it 'is host with port' do
        expect(address.to_s).to eql('127.0.0.1:27017')
      end
    end

    context 'address with ipv4 host and port' do
      let(:address) { Mongo::Address.new('127.0.0.1:27000') }

      it 'is host with port' do
        expect(address.to_s).to eql('127.0.0.1:27000')
      end
    end

    context 'address with ipv6 host only' do
      let(:address) { Mongo::Address.new('::1') }

      it 'is host with port' do
        expect(address.to_s).to eql('[::1]:27017')
      end
    end

    context 'address with ipv6 host and port' do
      let(:address) { Mongo::Address.new('[::1]:27000') }

      it 'is host with port' do
        expect(address.to_s).to eql('[::1]:27000')
      end
    end
  end
end

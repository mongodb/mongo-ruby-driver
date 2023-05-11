# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe Mongo::Address::IPv6 do

  let(:resolver) do
    described_class.new(*described_class.parse(address))
  end

  describe 'self.parse' do

    context 'when a port is provided' do

      it 'returns the host and port' do
        expect(described_class.parse('[::1]:27017')).to eq(['::1', 27017])
      end
    end

    context 'when no port is provided and host is in brackets' do

      it 'returns the host and port' do
        expect(described_class.parse('[::1]')).to eq(['::1', 27017])
      end
    end

    context 'when no port is provided and host is not in brackets' do

      it 'returns the host and port' do
        expect(described_class.parse('::1')).to eq(['::1', 27017])
      end
    end

    context 'when invalid address is provided' do

      it 'raises ArgumentError' do
        expect do
          described_class.parse('::1:27017')
        end.to raise_error(ArgumentError, 'Invalid IPv6 address: ::1:27017')
      end

      it 'rejects extra data around the address' do
        expect do
          described_class.parse('[::1]:27017oh')
        end.to raise_error(ArgumentError, 'Invalid IPv6 address: [::1]:27017oh')
      end

      it 'rejects bogus data in brackets' do
        expect do
          described_class.parse('[::hello]:27017')
        end.to raise_error(ArgumentError, 'Invalid IPv6 address: [::hello]:27017')
      end
    end
  end

  describe '#initialize' do

    context 'when a port is provided' do

      let(:address) do
        '[::1]:27017'
      end

      it 'sets the port' do
        expect(resolver.port).to eq(27017)
      end

      it 'sets the host' do
        expect(resolver.host).to eq('::1')
      end
    end

    context 'when no port is provided' do

      let(:address) do
        '[::1]'
      end

      it 'sets the port to 27017' do
        expect(resolver.port).to eq(27017)
      end

      it 'sets the host' do
        expect(resolver.host).to eq('::1')
      end
    end
  end

  describe '#socket' do

    # In JRuby 9.3.2.0 Socket::PF_INET6 is nil, causing IPv6 tests to fail.
    # https://github.com/jruby/jruby/issues/7069
    # JRuby 9.2 works correctly, this test is skipped on all JRuby versions
    # because we intend to remove JRuby support altogether and therefore
    # adding logic to condition on JRuby versions does not make sense.
    fails_on_jruby

    let(:address) do
      '[::1]'
    end

    context 'when ssl options are provided' do

      let(:socket) do
        resolver.socket(5, :ssl => true)
      end

      it 'returns an ssl socket' do
        allow_any_instance_of(Mongo::Socket::SSL).to receive(:connect!)
        expect(socket).to be_a(Mongo::Socket::SSL)
      end

      it 'sets the family as ipv6' do
        allow_any_instance_of(Mongo::Socket::SSL).to receive(:connect!)
        expect(socket.family).to eq(Socket::PF_INET6)
      end
    end

    context 'when ssl options are not provided' do

      let(:socket) do
        resolver.socket(5)
      end

      it 'returns a tcp socket' do
        allow_any_instance_of(Mongo::Socket::TCP).to receive(:connect!)
        expect(socket).to be_a(Mongo::Socket::TCP)
      end

      it 'sets the family a ipv6' do
        allow_any_instance_of(Mongo::Socket::TCP).to receive(:connect!)
        expect(socket.family).to eq(Socket::PF_INET6)
      end
    end
  end
end

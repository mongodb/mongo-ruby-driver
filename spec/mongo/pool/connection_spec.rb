require 'spec_helper'

include Mongo::Pool
include Mongo::Pool::Socket

describe Mongo::Pool::Connection do

  let(:host) { 'localhost' }
  let(:port) { 12345 }
  let(:timeout) { 2 }
  let(:opts) { { :connect => false } }
  let(:connection) { described_class.new(host, port, nil, opts) }

  before do
    allow_any_instance_of(Socket::TCP).to receive(:connect) { double(TCP) }
    allow_any_instance_of(Socket::TCP).to receive(:close)
    allow_any_instance_of(Socket::SSL).to receive(:connect) { double(SSL) }
    allow_any_instance_of(Socket::Unix).to receive(:connect) { double(Unix) }
    allow_any_instance_of(
      described_class).to receive(:connect).and_call_original
  end

  describe '#initialize' do

    it 'sets the host value' do
      expect(connection.host).to eq(host)
    end

    it 'sets the port value' do
      expect(connection.port).to eq(port)
    end

    it 'sets the default timeout value' do
      expect(connection.timeout).to eq(Connection::DEFAULT_TIMEOUT)
    end

    it 'sets the last use to nil' do
      expect(connection.last_use).to be nil
    end

    it 'sets the socket to nil' do
      socket = connection.instance_variable_get(:@socket)
      expect(socket).to be nil
    end

    context 'when timeout is specified' do

      let(:connection) { described_class.new(host, port, timeout, opts) }

      it 'sets the timeout value' do
        expect(connection.timeout).to eq(timeout)
      end

    end

    context 'when port is nil' do

      it 'creates unix socket' do
      end

    end

    context 'when options are provided' do

      context 'when :connect => false' do

        let(:opts) { { :connect => false } }

        it 'does not invoke connect' do
          conn = described_class.new(host, port, timeout, opts)
          expect(conn).to_not receive(:connect)
        end

        it 'socket remains nil' do
          conn = described_class.new(host, port, timeout, opts)
          socket = conn.instance_variable_get(:@socket)
          expect(socket).to be nil
        end

      end

      context 'when :connect => true' do

        let(:opts) { { :connect => true } }

        it 'invokes connect' do
          conn = described_class.new(host, port, timeout, opts)
          expect(conn).to have_received(:connect)
        end

        it 'socket is not nil' do
          conn = described_class.new(host, port, timeout, opts)
          socket = conn.instance_variable_get(:@socket)
          expect(socket).to_not be nil
        end

      end

      context 'when :connect is not specified' do

        let(:opts) { Hash.new }

        it 'invokes connect' do
          conn = described_class.new(host, port, timeout, opts)
          expect(conn).to have_received(:connect)
        end

        it 'socket is not nil' do
          conn = described_class.new(host, port, timeout, opts)
          socket = conn.instance_variable_get(:@socket)
          expect(socket).to_not be nil
        end

      end

      context 'when ssl options are specified' do

        let(:opts) { { :ssl => true, :connect => false } }

        it 'sets the @ssl_opts value' do
          conn = described_class.new(host, port, timeout, opts)
          ssl_opts = conn.instance_variable_get(:@ssl_opts)
          expect(ssl_opts).to_not be nil
          expect(ssl_opts.keys).to include :ssl
        end

      end

    end

  end

  describe '#lease' do

    it 'sets the lease time to now' do
      connection.lease
      expect(connection.last_use).to_not be nil
    end

  end

  describe '#expire' do

    it 'sets the lease time to nil' do
      connection.lease
      connection.expire
      expect(connection.last_use).to be nil
    end

  end

  describe '#expired?' do

    it 'returns true when lease time is not set' do
      connection.expire
      expect(connection.expired?).to be true
    end

    it 'returns false when the lease time has been set' do
      connection.lease
      expect(connection.expired?).to be false
    end

  end

  describe '#connect' do

    context 'when port is not set' do

      it 'creates a unix socket instance' do
        allow(Socket::Unix).to receive(:new)
        expect(Socket::Unix).to receive(:new)
        described_class.new(host, nil)
      end

    end

    context 'when ssl_opts are present' do

      let(:opts) { { :ssl => true } }

      it 'creates a ssl socket instance' do
        allow(Socket::SSL).to receive(:new)
        expect(Socket::SSL).to receive(:new)
        described_class.new(host, port, nil, opts)
      end

    end

    it 'creates a tcp socket instance by default' do
      allow(Socket::TCP).to receive(:new)
      expect(Socket::TCP).to receive(:new)
      described_class.new(host, port)
    end

  end

  describe '#disconnect' do

    let(:connection) { described_class.new(host, port) }

    context 'when the socket has not been set' do

      let(:opts) { { :connect => false } }
      let(:connection) { described_class.new(host, port, nil, opts) }

      it 'does not try to close the socket' do
        expect(Socket::TCP).to_not receive(:close)
        connection.disconnect
      end

    end

    context 'when the socket has been set' do

      it 'will try to close the socket' do
        socket = connection.instance_variable_get(:@socket)
        expect(socket).to receive(:close)
        connection.disconnect
      end

    end

    it 'sets the socket to nil' do
      connection.disconnect
      socket = connection.instance_variable_get(:@socket)
      expect(socket).to be nil
    end

  end

  describe '#read' do
    # TODO: definite Operation and OperationResult so that we can figure out
    # what needs to happen here.
  end

  describe '#write' do
    # TODO: definite Operation and OperationResult so that we can figure out
    # what needs to happen here.
  end

end

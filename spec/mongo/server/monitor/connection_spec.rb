require 'spec_helper'

describe Mongo::Server::Monitor::Connection do
  clean_slate

  let(:address) do
    Mongo::Address.new(ClusterConfig.instance.primary_address_str, options)
  end

  declare_topology_double

  let(:cluster) do
    double('cluster').tap do |cl|
      allow(cl).to receive(:topology).and_return(topology)
      allow(cl).to receive(:app_metadata).and_return(Mongo::Server::Monitor::AppMetadata.new({}))
      allow(cl).to receive(:options).and_return({})
      allow(cl).to receive(:heartbeat_interval).and_return(1000)
      allow(cl).to receive(:run_sdam_flow)
    end
  end

  let(:server) do
    Mongo::Server.new(address,
      cluster,
      Mongo::Monitoring.new,
      Mongo::Event::Listeners.new, {monitoring_io: false}.update(options))
  end

  let(:monitor) do
    register_background_thread_object(
      Mongo::Server::Monitor.new(server, server.event_listeners, server.monitoring,
        {
          app_metadata: Mongo::Server::Monitor::AppMetadata.new(options),
        }.update(options))
    ).tap do |monitor|
      monitor.run!
    end
  end

  let(:connection) do
    # NB this connection is set up in the background thread,
    # when the :scan option to client is changed to default to false
    # we must wait here for the connection to be established.
    # Do not call connect! on this connection as then the main thread
    # will be racing the monitoring thread to connect.
    monitor.connection.tap do |connection|
      expect(connection).not_to be nil

      deadline = Time.now + 5
      while Time.now < deadline
        if connection.send(:socket)
          break
        end
        sleep 0.1
      end
      expect(connection.send(:socket)).not_to be nil
    end
  end

  context 'when a connect_timeout is in the options' do

    context 'when a socket_timeout is in the options' do

      let(:options) do
        SpecConfig.instance.test_options.merge(connect_timeout: 3, socket_timeout: 5)
      end

      it 'uses the connect_timeout for the address' do
        expect(connection.address.options[:connect_timeout]).to eq(3)
      end

      it 'uses the connect_timeout as the socket_timeout' do
        expect(connection.send(:socket).timeout).to eq(3)
      end
    end

    context 'when a socket_timeout is not in the options' do

      let(:options) do
        SpecConfig.instance.test_options.merge(connect_timeout: 3, socket_timeout: nil)
      end

      it 'uses the connect_timeout for the address' do
        expect(connection.address.options[:connect_timeout]).to eq(3)
      end

      it 'uses the connect_timeout as the socket_timeout' do
        expect(connection.send(:socket).timeout).to eq(3)
      end
    end
  end

  context 'when a connect_timeout is not in the options' do

    context 'when a socket_timeout is in the options' do

      let(:options) do
        SpecConfig.instance.test_options.merge(connect_timeout: nil, socket_timeout: 5)
      end

      it 'does not specify connect_timeout for the address' do
        expect(connection.address.options[:connect_timeout]).to be nil
      end

      it 'uses the connect_timeout as the socket_timeout' do
        expect(connection.send(:socket).timeout).to eq(10)
      end
    end

    context 'when a socket_timeout is not in the options' do

      let(:options) do
        SpecConfig.instance.test_options.merge(connect_timeout: nil, socket_timeout: nil)
      end

      it 'does not specify connect_timeout for the address' do
        expect(connection.address.options[:connect_timeout]).to be nil
      end

      it 'uses the connect_timeout as the socket_timeout' do
        expect(connection.send(:socket).timeout).to eq(10)
      end
    end
  end

  describe '#ismaster' do
    let(:options) do
      SpecConfig.instance.test_options
    end

    let(:result) { connection.ismaster }

    it 'returns a hash' do
      expect(result).to be_a(Hash)
    end

    it 'is successful' do
      expect(result['ok']).to eq(1.0)
    end

    context 'network error during ismaster' do
      let(:result) do
        connection

        socket = connection.send(:socket).send(:socket)
        expect([Socket, OpenSSL::SSL::SSLSocket]).to include(socket.class)

        expect(socket).to receive(:write).and_raise(IOError)
        expect(socket).to receive(:write).and_call_original

        connection.ismaster
      end

      before do
        # Since we set expectations on the connection, kill the
        # background thread (but without disconnecting the connection).
        # Note also that we need to have the connection connected in
        # the first place, thus the scan! call here.
        monitor.scan!
        monitor.instance_variable_get('@thread').kill
        monitor.instance_variable_get('@thread').join
      end

      it 'retries ismaster and is successful' do
        expect(result).to be_a(Hash)
        expect(result['ok']).to eq(1.0)
      end

      it 'logs the retry' do
        expect(Mongo::Logger.logger).to receive(:warn) do |msg|
          expect(msg).to match(/Retrying ismaster in monitor for #{connection.address}/)
        end
        expect(result).to be_a(Hash)
      end

      it 'adds server diagnostics' do
        expect(Mongo::Logger.logger).to receive(:warn) do |msg|
          # The "on <address>" and "for <address>" bits are in different parts
          # of the message.
          expect(msg).to match(/on #{connection.address}/)
        end
        expect(result).to be_a(Hash)
      end
    end
  end

  describe '#connect!' do
    context 'network error' do
      before do
        address
        monitor.instance_variable_get('@thread').kill
        monitor.connection.disconnect!
      end

      let(:options) { SpecConfig.instance.test_options }

      let(:expected_message) { "MONGODB | Failed to handshake with #{address}: Mongo::Error::SocketError: test error" }

      it 'logs a warning' do
        # Note: the mock call below could mock do_write and raise IOError.
        # It is correct in raising Error::SocketError if mocking write
        # which performs exception mapping.
        expect_any_instance_of(Mongo::Socket).to receive(:write).and_raise(Mongo::Error::SocketError, 'test error')

        messages = []
        expect(Mongo::Logger.logger).to receive(:warn) do |msg|
          messages << msg
        end

        expect do
          monitor.connection.connect!
        end.to raise_error(Mongo::Error::SocketError, /test error/)

        messages.any? { |msg| msg.include?(expected_message) }.should be true
      end

      it 'adds server diagnostics' do
        # Note: the mock call below could mock do_write and raise IOError.
        # It is correct in raising Error::SocketError if mocking write
        # which performs exception mapping.
        expect_any_instance_of(Mongo::Socket).to receive(:write).and_raise(Mongo::Error::SocketError, 'test error')

        expect do
          monitor.connection.connect!
        end.to raise_error(Mongo::Error::SocketError, /on #{monitor.connection.address}/)
      end
    end
  end
end

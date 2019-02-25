require 'spec_helper'

describe Mongo::Server::Monitor::Connection do

  before do
    ClientRegistry.instance.close_all_clients
  end

  let(:client) do
    authorized_client.with(options)
  end

  let(:address) do
    client.cluster.next_primary.address
  end

  declare_topology_double

  let(:cluster) do
    double('cluster').tap do |cl|
      allow(cl).to receive(:topology).and_return(topology)
      allow(cl).to receive(:app_metadata).and_return(Mongo::Server::Monitor::AppMetadata.new(authorized_client.cluster.options))
      allow(cl).to receive(:options).and_return({})
    end
  end

  let(:server) do
    Mongo::Server.new(address,
                      cluster,
                      Mongo::Monitoring.new,
                      Mongo::Event::Listeners.new, options)
  end

  let(:connection) do
    # NB this connection is set up in the background thread,
    # when the :scan option to client is changed to default to false
    # we must wait here for the connection to be established.
    # Do not call connect! on this connection as then the main thread
    # will be racing the monitoring thread to connect.
    server.monitor.connection.tap do |connection|
      expect(connection).not_to be nil

      deadline = Time.now + 1
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
        expect(connection.address.send(:connect_timeout)).to eq(3)
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
        expect(connection.address.send(:connect_timeout)).to eq(3)
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

      it 'uses the default connect_timeout for the address' do
        expect(connection.address.send(:connect_timeout)).to eq(10)
      end

      it 'uses the connect_timeout as the socket_timeout' do
        expect(connection.send(:socket).timeout).to eq(10)
      end
    end

    context 'when a socket_timeout is not in the options' do

      let(:options) do
        SpecConfig.instance.test_options.merge(connect_timeout: nil, socket_timeout: nil)
      end

      it 'uses the default connect_timeout for the address' do
        expect(connection.address.send(:connect_timeout)).to eq(10)
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

      it 'retries ismaster and is successful' do
        expect(result).to be_a(Hash)
        expect(result['ok']).to eq(1.0)
      end

      it 'logs the retry' do
        expect(Mongo::Logger.logger).to receive(:warn) do |msg|
          expect(msg).to match(/Retrying ismaster on #{connection.address}/)
        end
        expect(result).to be_a(Hash)
      end
    end
  end

  describe '#connect!' do
    context 'network error' do
      before do
        address
        server.monitor.instance_variable_get('@thread').kill
        server.monitor.connection.disconnect!
        expect_any_instance_of(Mongo::Socket).to receive(:write).and_raise(Mongo::Error::SocketError, 'test error')
      end

      let(:options) { SpecConfig.instance.test_options }

      let(:expected_message) { "MONGODB | Failed to handshake with #{address}: Mongo::Error::SocketError: test error" }

      it 'logs a warning' do
        expect(Mongo::Logger.logger).to receive(:warn).with(expected_message).and_call_original
        expect do
          server.monitor.connection.connect!
        end.to raise_error(Mongo::Error::SocketError, 'test error')
      end
    end
  end
end

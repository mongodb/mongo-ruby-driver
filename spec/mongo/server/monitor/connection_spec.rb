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
      monitor.scan!
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
end

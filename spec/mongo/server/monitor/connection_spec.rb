# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe Mongo::Server::Monitor::Connection do
  clean_slate

  let(:address) do
    Mongo::Address.new(ClusterConfig.instance.primary_address_str, options)
  end

  declare_topology_double

  let(:monitor_app_metadata) do
    Mongo::Server::Monitor::AppMetadata.new(
      server_api: SpecConfig.instance.ruby_options[:server_api],
    )
  end

  let(:cluster) do
    double('cluster').tap do |cluster|
      allow(cluster).to receive(:topology).and_return(topology)
      allow(cluster).to receive(:app_metadata).and_return(Mongo::Server::Monitor::AppMetadata.new({}))
      allow(cluster).to receive(:options).and_return({})
      allow(cluster).to receive(:monitor_app_metadata).and_return(monitor_app_metadata)
      allow(cluster).to receive(:push_monitor_app_metadata).and_return(monitor_app_metadata)
      allow(cluster).to receive(:heartbeat_interval).and_return(1000)
      allow(cluster).to receive(:run_sdam_flow)
    end
  end

  let(:server) do
    Mongo::Server.new(address,
      cluster,
      Mongo::Monitoring.new,
      Mongo::Event::Listeners.new, {monitoring_io: false}.update(options))
  end

  let(:monitor) do
    metadata = Mongo::Server::Monitor::AppMetadata.new(options)
    register_background_thread_object(
      Mongo::Server::Monitor.new(server, server.event_listeners, server.monitoring,
        {
          app_metadata: metadata,
          push_monitor_app_metadata: metadata,
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

      deadline = Mongo::Utils.monotonic_time + 5
      while Mongo::Utils.monotonic_time < deadline
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

  describe '#connect!' do

    let(:options) do
      SpecConfig.instance.test_options.merge(
        app_metadata: monitor_app_metadata,
      )
    end

    context 'when address resolution fails' do
      let(:connection) { described_class.new(server.address, options) }

      it 'propagates the exception' do
        connection

        expect(Socket).to receive(:getaddrinfo).and_raise(SocketError.new('Test exception'))
        lambda do
          connection.connect!
        end.should raise_error(SocketError, 'Test exception')
      end
    end
  end

  describe '#check_document' do
    context 'with API version' do
      let(:meta) do
        Mongo::Server::AppMetadata.new({
          server_api: { version: '1' }
        })
      end

      [false, true].each do |hello_ok|
        it "returns hello document if server #{ if hello_ok then 'supports' else 'does not support' end } hello" do
          subject = described_class.new(double("address"), app_metadata: meta)
          expect(subject).to receive(:hello_ok?).and_return(hello_ok)
          document = subject.check_document
          expect(document['hello']).to eq(1)
        end
      end
    end

    context 'without API version' do
      let(:meta) { Mongo::Server::AppMetadata.new({}) }

      it 'returns legacy hello document' do
        subject = described_class.new(double("address"), app_metadata: meta)
        expect(subject).to receive(:hello_ok?).and_return(false)
        document = subject.check_document
        expect(document['isMaster']).to eq(1)
      end

      it 'returns hello document when server responded with helloOk' do
        subject = described_class.new(double("address"), app_metadata: meta)
        expect(subject).to receive(:hello_ok?).and_return(true)
        document = subject.check_document
        expect(document['hello']).to eq(1)
      end
    end
  end
end

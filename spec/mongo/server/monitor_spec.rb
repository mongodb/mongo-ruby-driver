# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe Mongo::Server::Monitor do
  before(:all) do
    ClientRegistry.instance.close_all_clients
  end

  let(:address) do
    default_address
  end

  let(:listeners) do
    Mongo::Event::Listeners.new
  end

  let(:monitor_options) do
    {}
  end

  let(:monitor_app_metadata) do
    Mongo::Server::Monitor::AppMetadata.new(
      server_api: SpecConfig.instance.ruby_options[:server_api],
    )
  end

  let(:cluster) do
    double('cluster').tap do |cluster|
      allow(cluster).to receive(:run_sdam_flow)
      allow(cluster).to receive(:heartbeat_interval).and_return(1000)
    end
  end

  let(:server) do
    Mongo::Server.new(address, cluster, Mongo::Monitoring.new, listeners,
      monitoring_io: false)
  end

  let(:monitor) do
    register_background_thread_object(
      described_class.new(server, listeners, Mongo::Monitoring.new,
        SpecConfig.instance.test_options.merge(cluster: cluster).merge(monitor_options).update(
          app_metadata: monitor_app_metadata,
          push_monitor_app_metadata: monitor_app_metadata))
    )
  end

  describe '#scan!' do

    context 'when calling multiple times in succession' do

      it 'throttles the scans to minimum 500ms' do
        start = Mongo::Utils.monotonic_time
        monitor.scan!
        monitor.scan!
        expect(Mongo::Utils.monotonic_time - start).to be >= 0.5
      end
    end

    context 'when the hello fails the first time' do

      let(:monitor_options) do
        {monitoring_io: false}
      end

      it 'runs sdam flow on unknown description' do
        expect(monitor).to receive(:check).once.and_raise(Mongo::Error::SocketError)
        expect(cluster).to receive(:run_sdam_flow)
        monitor.scan!
      end
    end

    context 'when the hello command succeeds' do

      it 'invokes sdam flow' do
        server.unknown!
        expect(server.description).to be_unknown

        updated_desc = nil
        expect(cluster).to receive(:run_sdam_flow) do |prev_desc, _updated_desc|
          updated_desc = _updated_desc
        end
        monitor.scan!

        expect(updated_desc).not_to be_unknown
      end
    end

    context 'when the hello command fails' do

      context 'when no server is running on the address' do

        let(:address) do
          Mongo::Address.new('127.0.0.1:27050')
        end

        before do
          server.unknown!
          expect(server.description).to be_unknown
          monitor.scan!
        end

        it 'keeps the server unknown' do
          expect(server.description).to be_unknown
        end
      end

      context 'when the socket gets an exception' do

        let(:address) do
          default_address
        end

        before do
          server.unknown!
          expect(server.description).to be_unknown
          expect(monitor).to receive(:check).and_raise(Mongo::Error::SocketError)
          monitor.scan!
        end

        it 'keeps the server unknown' do
          expect(server.description).to be_unknown
        end

        it 'disconnects the connection' do
          expect(monitor.connection).to be nil
        end
      end
    end
  end

=begin heartbeat interval is now taken out of cluster, monitor has no useful options
  describe '#heartbeat_frequency' do

    context 'when an option is provided' do

      let(:monitor_options) do
        {:heartbeat_frequency => 5}
      end

      it 'returns the option' do
        expect(monitor.heartbeat_frequency).to eq(5)
      end
    end

    context 'when no option is provided' do

      let(:monitor_options) do
        {:heartbeat_frequency => nil}
      end

      it 'defaults to 10' do
        expect(monitor.heartbeat_frequency).to eq(10)
      end
    end
  end
=end

  describe '#run!' do

    let!(:thread) do
      monitor.run!
    end

    context 'when the monitor is already running' do

      it 'does not create a new thread' do
        expect(monitor.restart!).to be(thread)
      end
    end

    context 'when the monitor is not already running' do

      before do
        monitor.stop!
        sleep(1)
      end

      it 'creates a new thread' do
        expect(monitor.restart!).not_to be(thread)
      end
    end

    context 'when running after a stop' do
      it 'starts the thread' do
        ClientRegistry.instance.close_all_clients
        sleep 1
        thread
        sleep 1

        RSpec::Mocks.with_temporary_scope do
          expect(monitor.connection).to receive(:disconnect!).and_call_original
          monitor.stop!
          sleep 1
          expect(thread.alive?).to be false
          new_thread = monitor.run!
          sleep 1
          expect(new_thread.alive?).to be(true)
        end
      end
    end
  end

  describe '#stop' do

    let(:thread) do
      monitor.run!
    end

    it 'kills the monitor thread' do
      ClientRegistry.instance.close_all_clients
      thread
      sleep 0.5

      RSpec::Mocks.with_temporary_scope do
        expect(monitor.connection).to receive(:disconnect!).and_call_original
        monitor.stop!
        expect(thread.alive?).to be(false)
      end
    end
  end

  describe '#connection' do

    context 'when there is a connect_timeout option set' do

      let(:connect_timeout) do
        1
      end

      let(:monitor_options) do
        {connect_timeout: connect_timeout}
      end

      it 'sets the value as the timeout on the connection' do
        monitor.scan!
        expect(monitor.connection.socket_timeout).to eq(connect_timeout)
      end

      it 'set the value as the timeout on the socket' do
        monitor.scan!
        expect(monitor.connection.send(:socket).timeout).to eq(connect_timeout)
      end
    end
  end

  describe '#log_warn' do
    it 'works' do
      expect do
        monitor.log_warn('test warning')
      end.not_to raise_error
    end
  end

  describe '#do_scan' do

    let(:result) { monitor.send(:do_scan) }

    it 'returns a hash' do
      expect(result).to be_a(Hash)
    end

    it 'is successful' do
      expect(result['ok']).to eq(1.0)
    end

    context 'network error during check' do
      let(:result) do
        expect(monitor).to receive(:check).and_raise(IOError)
        # The retry is done on a new socket instance.
        #expect(socket).to receive(:write).and_call_original
        monitor.send(:do_scan)
      end

      it 'adds server diagnostics' do
        expect(Mongo::Logger.logger).to receive(:warn) do |msg|
          # The "on <address>" and "for <address>" bits are in different parts
          # of the message.
          expect(msg).to match(/#{server.address}/)
        end
        expect do
          result
        end.to raise_error(IOError)
      end
    end

    context 'network error during connection' do
      let(:options) { SpecConfig.instance.test_options }

      let(:expected_message) { "MONGODB | Failed to handshake with #{address}: Mongo::Error::SocketError: test error" }

      before do
        monitor.connection.should be nil
      end

      it 'logs a warning' do
        # Note: the mock call below could mock do_write and raise IOError.
        # It is correct in raising Error::SocketError if mocking write
        # which performs exception mapping.
        expect_any_instance_of(Mongo::Socket).to receive(:write).and_raise(Mongo::Error::SocketError, 'test error')

        messages = []
        expect(Mongo::Logger.logger).to receive(:warn).at_least(:once) do |msg|
          messages << msg
        end

        monitor.scan!.should be_unknown

        messages.any? { |msg| msg.include?(expected_message) }.should be true
      end

      it 'adds server diagnostics' do
        # Note: the mock call below could mock do_write and raise IOError.
        # It is correct in raising Error::SocketError if mocking write
        # which performs exception mapping.
        expect_any_instance_of(Mongo::Socket).to receive(:write).and_raise(Mongo::Error::SocketError, 'test error')

        expect do
          monitor.send(:check)
        end.to raise_error(Mongo::Error::SocketError, /#{server.address}/)
      end
    end
  end
end

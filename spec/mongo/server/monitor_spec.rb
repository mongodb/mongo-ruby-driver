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
    register_server_monitor(
      described_class.new(server, listeners, Mongo::Monitoring.new,
        SpecConfig.instance.test_options.merge(cluster: cluster).merge(monitor_options))
    )
  end

  describe '#scan!' do

    context 'when calling multiple times in succession' do

      it 'throttles the scans to minimum 500ms' do
        start = Time.now
        monitor.scan!
        monitor.scan!
        expect(Time.now - start).to be >= 0.5
      end
    end

    context 'when the ismaster fails the first time' do

      let(:monitor_options) do
        {monitoring_io: false}
      end

      let(:socket) do
        monitor.connection.connect!
        monitor.connection.__send__(:socket)
      end

      it 'retries the ismaster' do
        expect(socket).to receive(:write).once.and_raise(Mongo::Error::SocketError)
        expect(socket).to receive(:write).and_call_original
        expect(cluster).to receive(:run_sdam_flow)
        monitor.scan!
      end
    end

    context 'when the ismaster command succeeds' do

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

    context 'when the ismaster command fails' do

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

        let(:socket) do
          monitor.connection.connect!
          monitor.connection.__send__(:socket)
        end

        before do
          expect(socket).to receive(:write).twice.and_raise(Mongo::Error::SocketError)
          server.unknown!
          expect(server.description).to be_unknown
          monitor.scan!
        end

        it 'keeps the server unknown' do
          expect(server.description).to be_unknown
        end

        it 'disconnects the connection' do
          expect(monitor.connection).to_not be_connected
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

  describe '#restart!' do

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
        expect(monitor.connection.timeout).to eq(connect_timeout)
      end

      it 'set the value as the timeout on the socket' do
        monitor.connection.connect!
        expect(monitor.connection.send(:socket).timeout).to eq(connect_timeout)
      end
    end
  end
end

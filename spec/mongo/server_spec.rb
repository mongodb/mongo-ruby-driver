# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe Mongo::Server do
  fails_on_jruby

  declare_topology_double

  let(:cluster) do
    double('cluster').tap do |cl|
      allow(cl).to receive(:topology).and_return(topology)
      allow(cl).to receive(:app_metadata).and_return(app_metadata)
      allow(cl).to receive(:options).and_return({})
    end
  end

  let(:listeners) do
    Mongo::Event::Listeners.new
  end

  let(:monitoring) do
    Mongo::Monitoring.new(monitoring: false)
  end

  let(:address) do
    default_address
  end

  let(:pool) do
    server.pool
  end

  let(:server_options) do
    {}
  end

  let(:server) do
    register_server(
      described_class.new(address, cluster, monitoring, listeners,
        SpecConfig.instance.test_options.merge(monitoring_io: false).merge(server_options))
    )
  end

  let(:monitor_app_metadata) do
    Mongo::Server::Monitor::AppMetadata.new(
      server_api: SpecConfig.instance.ruby_options[:server_api],
    )
  end

  shared_context 'with monitoring io' do
    let(:server_options) do
      {monitoring_io: true}
    end

    before do
      allow(cluster).to receive(:monitor_app_metadata).and_return(monitor_app_metadata)
      allow(cluster).to receive(:push_monitor_app_metadata).and_return(monitor_app_metadata)
      allow(cluster).to receive(:heartbeat_interval).and_return(1000)
    end
  end

  describe '#==' do

    context 'when the other is not a server' do

      let(:other) do
        false
      end

      it 'returns false' do
        expect(server).to_not eq(other)
      end
    end

    context 'when the other is a server' do

      context 'when the addresses match' do

        let(:other) do
          register_server(
            described_class.new(address, cluster, monitoring, listeners,
              SpecConfig.instance.test_options.merge(monitoring_io: false))
          )
        end

        it 'returns true' do
          expect(server).to eq(other)
        end
      end

      context 'when the addresses dont match' do

        let(:other_address) do
          Mongo::Address.new('127.0.0.1:27018')
        end

        let(:other) do
          register_server(
            described_class.new(other_address, cluster, monitoring, listeners,
              SpecConfig.instance.test_options.merge(monitoring_io: false))
          )
        end

        it 'returns false' do
          expect(server).to_not eq(other)
        end
      end
    end
  end

  describe '#disconnect!' do

    context 'with monitoring io' do
      include_context 'with monitoring io'

      it 'stops the monitor instance' do
        expect(server.instance_variable_get(:@monitor)).to receive(:stop!).and_call_original
        server.disconnect!
      end
    end

    context 'when server has a pool' do
      before do
        allow(server).to receive(:unknown?).and_return(false)
        allow(cluster).to receive(:run_sdam_flow)
        server.pool.ready
      end

      it 'pauses and clears the connection pool' do
        expect(server.pool_internal).to receive(:close).once.and_call_original
        RSpec::Mocks.with_temporary_scope do
          # you can't disconnect from a known server, since this pauses the
          # pool and we only want to pause the pools of unknown servers.
          server.unknown!
          allow(server).to receive(:unknown?).and_return(true)
          server.close
        end
      end
    end

    context 'when server reconnects' do
      before do
        allow(server).to receive(:unknown?).and_return(false)
        allow(cluster).to receive(:run_sdam_flow)
        server.pool.ready
      end

      it 'keeps the same pool' do
        pool = server.pool
        RSpec::Mocks.with_temporary_scope do
          # you can't disconnect from a known server, since this pauses the
          # pool and we only want to pause the pools of unknown servers.
          # server.unknown!
          allow(server).to receive(:unknown?).and_return(true)
          server.disconnect!
        end
        server.reconnect!
        allow(server).to receive(:unknown?).and_return(false)
        expect(server.pool).to eq(pool)
      end
    end
  end

  describe '#initialize' do
    include_context 'with monitoring io'

    before do
      allow(cluster).to receive(:run_sdam_flow)
    end

    it 'sets the address host' do
      expect(server.address.host).to eq(default_address.host)
    end

    it 'sets the address port' do
      expect(server.address.port).to eq(default_address.port)
    end

    it 'sets the options' do
      expect(server.options[:monitoring_io]).to be true
    end

    context 'with monitoring app metadata option' do
      require_no_required_api_version

      it 'creates monitor with monitoring app metadata' do
        server.monitor.scan!
        expect(server.monitor.connection.options[:app_metadata]).to be monitor_app_metadata
      end
    end

    context 'monitoring_io: false' do

      let(:server_options) do
        {monitoring_io: false}
      end

      it 'does not create monitoring thread' do
        expect(server.monitor.instance_variable_get('@thread')).to be nil
      end
    end

    context 'monitoring_io: true' do
      include_context 'with monitoring io'

      it 'creates monitoring thread' do
        expect(server.monitor.instance_variable_get('@thread')).to be_a(Thread)
      end
    end
  end

  describe '#scan!' do
    clean_slate

    include_context 'with monitoring io'

    before do
      # We are invoking scan! on the monitor manually, stop the background
      # thread to avoid it interfering with our assertions.
      server.monitor.stop!
    end

    it 'delegates scan to the monitor' do
      expect(server.monitor).to receive(:scan!)
      server.scan!
    end

    it 'invokes sdam flow eventually' do
      expect(cluster).to receive(:run_sdam_flow)
      server.scan!
    end
  end

  describe '#reconnect!' do
    include_context 'with monitoring io'

    before do
      expect(server.monitor).to receive(:restart!).and_call_original
    end

    it 'restarts the monitor and returns true' do
      expect(server.reconnect!).to be(true)
    end
  end

  describe 'retry_writes?' do

    before do
      allow(server).to receive(:features).and_return(features)
    end

    context 'when the server version is less than 3.6' do

      let(:features) do
        double('features', sessions_enabled?: false)
      end

      context 'when the server has a logical_session_timeout value' do

        before do
          allow(server).to receive(:logical_session_timeout).and_return(true)
        end

        it 'returns false' do
          expect(server.retry_writes?).to be(false)
        end
      end

      context 'when the server does not have a logical_session_timeout value' do

        before do
          allow(server).to receive(:logical_session_timeout).and_return(nil)
        end

        it 'returns false' do
          expect(server.retry_writes?).to be(false)
        end
      end
    end

    context 'when the server version is at least 3.6' do

      let(:features) do
        double('features', sessions_enabled?: true)
      end

      context 'when the server has a logical_session_timeout value' do

        before do
          allow(server).to receive(:logical_session_timeout).and_return(true)
        end

        context 'when the server is a standalone' do

          before do
            allow(server).to receive(:standalone?).and_return(true)
          end

          it 'returns false' do
            expect(server.retry_writes?).to be(false)
          end
        end

        context 'when the server is not a standalone' do

          before do
            allow(server).to receive(:standalone?).and_return(true)
          end

          it 'returns false' do
            expect(server.retry_writes?).to be(false)
          end
        end
      end

      context 'when the server does not have a logical_session_timeout value' do

        before do
          allow(server).to receive(:logical_session_timeout).and_return(nil)
        end

        it 'returns false' do
          expect(server.retry_writes?).to be(false)
        end
      end
    end
  end

  describe '#summary' do
    context 'server is primary' do
      let(:server) do
        make_server(:primary)
      end

      before do
        expect(server).to be_primary
      end

      it 'includes its status' do
        expect(server.summary).to match(/PRIMARY/)
      end

      it 'includes replica set name' do
        expect(server.summary).to match(/replica_set=mongodb_set/)
      end
    end

    context 'server is secondary' do
      let(:server) do
        make_server(:secondary)
      end

      before do
        expect(server).to be_secondary
      end

      it 'includes its status' do
        expect(server.summary).to match(/SECONDARY/)
      end

      it 'includes replica set name' do
        expect(server.summary).to match(/replica_set=mongodb_set/)
      end
    end

    context 'server is arbiter' do
      let(:server) do
        make_server(:arbiter)
      end

      before do
        expect(server).to be_arbiter
      end

      it 'includes its status' do
        expect(server.summary).to match(/ARBITER/)
      end

      it 'includes replica set name' do
        expect(server.summary).to match(/replica_set=mongodb_set/)
      end
    end

    context 'server is ghost' do
      let(:server) do
        make_server(:ghost)
      end

      before do
        expect(server).to be_ghost
      end

      it 'includes its status' do
        expect(server.summary).to match(/GHOST/)
      end

      it 'does not include replica set name' do
        expect(server.summary).not_to include('replica_set')
      end
    end

    context 'server is other' do
      let(:server) do
        make_server(:other)
      end

      before do
        expect(server).to be_other
      end

      it 'includes its status' do
        expect(server.summary).to match(/OTHER/)
      end

      it 'includes replica set name' do
        expect(server.summary).to match(/replica_set=mongodb_set/)
      end
    end

    context 'server is unknown' do

      let(:server_options) do
        {monitoring_io: false}
      end

      before do
        expect(server).to be_unknown
      end

      it 'includes unknown status' do
        expect(server.summary).to match(/UNKNOWN/)
      end

      it 'does not include replica set name' do
        expect(server.summary).not_to include('replica_set')
      end
    end

    context 'server is a mongos' do
      let(:server) do
        make_server(:mongos)
      end

      before do
        expect(server).to be_mongos
      end

      it 'specifies the server is a mongos' do
        expect(server.summary).to match(/MONGOS/)
      end
    end
  end

  describe '#log_warn' do
    it 'works' do
      expect do
        server.log_warn('test warning')
      end.not_to raise_error
    end
  end
end

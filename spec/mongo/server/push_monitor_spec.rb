# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe Mongo::Server::PushMonitor do
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
      Mongo::Server::Monitor.new(server, listeners, Mongo::Monitoring.new,
        SpecConfig.instance.test_options.merge(cluster: cluster).merge(monitor_options).update(
          app_metadata: monitor_app_metadata,
          push_monitor_app_metadata: monitor_app_metadata))
    )
  end

  let(:topology_version) do
    Mongo::TopologyVersion.new('processId' => BSON::ObjectId.new, 'counter' => 1)
  end

  let(:check_document) do
    {hello: 1}
  end

  let(:push_monitor) do
    described_class.new(monitor, topology_version, monitor.monitoring,
      **monitor.options.merge(check_document: check_document))
  end

  describe '#do_work' do
    it 'works' do
      lambda do
        push_monitor.do_work
      end.should_not raise_error
    end

    context 'network error during check' do
      it 'does not propagate the exception' do
        push_monitor

        expect(Socket).to receive(:getaddrinfo).and_raise(SocketError.new('Test exception'))
        lambda do
          push_monitor.do_work
        end.should_not raise_error
      end

      it 'stops the monitoring' do
        push_monitor

        start = Mongo::Utils.monotonic_time

        expect(Socket).to receive(:getaddrinfo).and_raise(SocketError.new('Test exception'))
        lambda do
          push_monitor.do_work
        end.should_not raise_error

        push_monitor.running?.should be false
      end
    end
  end

end

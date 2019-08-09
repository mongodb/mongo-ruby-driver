require 'spec_helper'

describe 'Cmap' do

  declare_topology_double

  let(:cluster) do
    double('cluster').tap do |cl|
      allow(cl).to receive(:topology).and_return(topology)
      allow(cl).to receive(:options).and_return({})
      allow(cl).to receive(:app_metadata).and_return(Mongo::Server::AppMetadata.new({}))
      allow(cl).to receive(:run_sdam_flow)
      allow(cl).to receive(:update_cluster_time)
      allow(cl).to receive(:cluster_time).and_return(nil)
    end
  end

  let(:options) do
    SpecConfig.instance.ssl_options.merge(SpecConfig.instance.compressor_options)
      .merge(SpecConfig.instance.retry_writes_options).merge(SpecConfig.instance.auth_options)
      .merge(monitoring_io: false)
  end

  CMAP_TESTS.each do |file|
    spec = Mongo::Cmap::Spec.new(file)

    context("#{spec.description} (#{file.sub(%r'.*/data/cmap/', '')})") do


      before do
        subscriber = EventSubscriber.new

        monitoring = Mongo::Monitoring.new(monitoring: false)
        monitoring.subscribe(Mongo::Monitoring::CONNECTION_POOL, subscriber)

        server = register_server(
          Mongo::Server.new(
            ClusterConfig.instance.primary_address,
            cluster,
            monitoring,
            Mongo::Event::Listeners.new,
            options.merge(spec.pool_options)
          ).tap do |server|
            allow(server).to receive(:description).and_return(ClusterConfig.instance.primary_description)
          end
        )
        spec.setup(server, subscriber)
      end

      let!(:result) do
        mock_socket = double('socket')
        allow(mock_socket).to receive(:close)
        allow_any_instance_of(Mongo::Server::Connection).to receive(:do_connect).and_return(mock_socket)
        spec.run
      end

      let(:verifier) do
        Mongo::Cmap::Verifier.new(spec)
      end

      it 'raises the correct error' do
        expect(result['error']).to eq(spec.expected_error)
      end

      let(:actual_events) { result['events'].freeze }

      it 'emits the correct number of events' do
        expect(actual_events.length).to eq(spec.expected_events.length)
      end

      spec.expected_events.each_with_index do |expected_event, index|
        it "emits correct event #{index+1}" do
          verifier.verify_hashes(actual_events[index], expected_event)
        end
      end
    end
  end
end

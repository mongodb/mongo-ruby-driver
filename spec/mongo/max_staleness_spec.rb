require 'spec_helper'

describe 'Max Staleness Spec' do

  include Mongo::ServerSelection::Read

  MAX_STALENESS_TESTS.each do |file|

    spec = Mongo::ServerSelection::Read::Spec.new(file)

    context(spec.description) do

      let(:topology) do
        spec.type.new({}, monitoring, [])
      end

      let(:monitoring) do
        Mongo::Monitoring.new(monitoring: false)
      end

      let(:listeners) do
        Mongo::Event::Listeners.new
      end

      let(:options) do
        if spec.heartbeat_frequency
          TEST_OPTIONS.merge(heartbeat_frequency: spec.heartbeat_frequency)
        else
          TEST_OPTIONS.dup.tap do |opts|
            opts.delete(:heartbeat_frequency)
          end
        end.merge!(server_selection_timeout: 0.2, connect_timeout: 0.1)
      end

      let(:cluster) do
        double('cluster').tap do |c|
          allow(c).to receive(:topology).and_return(topology)
          allow(c).to receive(:single?).and_return(topology.single?)
          allow(c).to receive(:sharded?).and_return(topology.sharded?)
          allow(c).to receive(:replica_set?).and_return(topology.replica_set?)
          allow(c).to receive(:unknown?).and_return(topology.unknown?)
          allow(c).to receive(:options).and_return(options)
          allow(c).to receive(:scan!).and_return(true)
          allow(c).to receive(:app_metadata).and_return(app_metadata)
        end
      end

      let(:candidate_servers) do
        spec.candidate_servers.collect do |server|
          features = double('features').tap do |feat|
            allow(feat).to receive(:max_staleness_enabled?).and_return(server['maxWireVersion'] && server['maxWireVersion'] >= 5)
          end
          address = Mongo::Address.new(server['address'])
          Mongo::Server.new(address, cluster, monitoring, listeners, options).tap do |s|
            allow(s).to receive(:average_round_trip_time).and_return(server['avg_rtt_ms'] / 1000.0) if server['avg_rtt_ms']
            allow(s).to receive(:tags).and_return(server['tags'])
            allow(s).to receive(:secondary?).and_return(server['type'] == 'RSSecondary')
            allow(s).to receive(:primary?).and_return(server['type'] == 'RSPrimary')
            allow(s).to receive(:connectable?).and_return(true)
            allow(s).to receive(:last_write_date).and_return(server['lastWrite']['lastWriteDate']['$numberLong'].to_i) if server['lastWrite']
            allow(s).to receive(:last_scan).and_return(server['lastUpdateTime'])
            allow(s).to receive(:features).and_return(features)
          end
        end
      end

      let(:in_latency_window) do
        spec.in_latency_window.collect do |server|
          Mongo::Server.new(Mongo::Address.new(server['address']), cluster, monitoring, listeners, options)
        end
      end

      let(:server_selector_definition) do
        { mode: spec.read_preference['mode'] }.tap do |definition|
          definition[:tag_sets] = spec.read_preference['tag_sets']
          definition[:max_staleness] = spec.max_staleness if spec.max_staleness
        end
      end

      let(:server_selector) do
        Mongo::ServerSelector.get(server_selector_definition)
      end

      before do
        allow(cluster).to receive(:servers).and_return(candidate_servers)
      end

      context 'when the max staleness is invalid' do

        it 'Raises an InvalidServerPreference exception', if: spec.invalid_max_staleness? do

          expect do
            server_selector.select_server(cluster)
          end.to raise_exception(Mongo::Error::InvalidServerPreference)
        end
      end

      context 'when the max staleness is valid' do

        context 'when there are available servers' do

          it 'Finds all suitable servers in the latency window', if: (spec.replica_set? && !spec.invalid_max_staleness? && spec.server_available?) do
            expect(server_selector.send(:select, cluster.servers)).to match_array(in_latency_window)
          end

          it 'Finds the most suitable server in the latency window', if: (!spec.invalid_max_staleness? && spec.server_available?) do
            expect(in_latency_window).to include(server_selector.select_server(cluster))
          end
        end

        context 'when there are no available servers', if: (!spec.invalid_max_staleness? && !spec.server_available?) do

          it 'Raises a NoServerAvailable Exception' do
            expect do
              server_selector.select_server(cluster)
            end.to raise_exception(Mongo::Error::NoServerAvailable)
          end
        end
      end
    end
  end
end

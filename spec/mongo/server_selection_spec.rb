require 'spec_helper'

describe 'Server Selection' do

  include Mongo::ServerSelection::Read

  SERVER_SELECTION_TESTS.each do |file|

    spec = Mongo::ServerSelection::Read::Spec.new(file)

    context(spec.description) do

      let(:monitoring) do
        Mongo::Monitoring.new(monitoring: false)
      end

      let(:topology) do
        spec.type.new({}, monitoring)
      end

      let(:listeners) do
        Mongo::Event::Listeners.new
      end

      let(:cluster) do
        double('cluster').tap do |c|
          allow(c).to receive(:topology).and_return(topology)
          allow(c).to receive(:single?).and_return(topology.single?)
          allow(c).to receive(:sharded?).and_return(topology.sharded?)
          allow(c).to receive(:replica_set?).and_return(topology.replica_set?)
          allow(c).to receive(:unknown?).and_return(topology.unknown?)
          allow(c).to receive(:app_metadata).and_return(app_metadata)
        end
      end

      let(:candidate_servers) do
        spec.candidate_servers.collect do |server|
          address = Mongo::Address.new(server['address'])
          Mongo::Server.new(address, cluster, monitoring, listeners, TEST_OPTIONS).tap do |s|
            allow(s).to receive(:average_round_trip_time).and_return(server['avg_rtt_ms'] / 1000.0)
            allow(s).to receive(:tags).and_return(server['tags'])
            allow(s).to receive(:secondary?).and_return(server['type'] == 'RSSecondary')
            allow(s).to receive(:primary?).and_return(server['type'] == 'RSPrimary')
            allow(s).to receive(:connectable?).and_return(true)
          end
        end
      end

      let(:in_latency_window) do
        spec.in_latency_window.collect do |server|
          address = Mongo::Address.new(server['address'])
          Mongo::Server.new(address, cluster, monitoring, listeners, TEST_OPTIONS).tap do |s|
            allow(s).to receive(:average_round_trip_time).and_return(server['avg_rtt_ms'] / 1000.0)
            allow(s).to receive(:tags).and_return(server['tags'])
            allow(s).to receive(:connectable?).and_return(true)
          end
        end
      end

      let(:server_selector) do
        Mongo::ServerSelector.get(:mode => spec.read_preference['mode'],
                                  :tag_sets => spec.read_preference['tag_sets'])
      end

      before do
        allow(cluster).to receive(:servers).and_return(candidate_servers)
        allow(cluster).to receive(:options).and_return(server_selection_timeout: 0.2)
        allow(cluster).to receive(:scan!).and_return(true)
        allow(cluster).to receive(:app_metadata).and_return(app_metadata)
      end

      context 'Valid read preference and matching server available', if: spec.server_available? do

        it 'Finds all suitable servers in the latency window', if: spec.replica_set? do
          expect(server_selector.send(:select, cluster.servers)).to match_array(in_latency_window)
        end

        it 'Finds the most suitable server in the latency window' do
          expect(in_latency_window).to include(server_selector.select_server(cluster))
        end
      end

      context 'No matching server available', if: !spec.server_available? do

        it 'Raises exception' do
          expect do
            server_selector.select_server(cluster)
          end.to raise_exception(Mongo::Error::NoServerAvailable)
        end
      end
    end
  end
end

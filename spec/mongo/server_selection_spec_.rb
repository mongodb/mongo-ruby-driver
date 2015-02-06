require 'spec_helper'

describe 'Server Selection' do

  include Mongo::ServerSelection::Read

  SERVER_SELECTION_TESTS.each do |file|

    spec = Mongo::ServerSelection::Read::Spec.new(file)

    context(spec.description) do

      let(:topology) do
        spec.type.new({})
      end

      let(:cluster) do
        double('cluster').tap do |c|
          allow(c).to receive(:topology).and_return(topology)
          allow(c).to receive(:standalone?).and_return(topology.standalone?)
          allow(c).to receive(:sharded?).and_return(topology.sharded?)
          allow(c).to receive(:replica_set?).and_return(topology.replica_set?)
        end
      end

      let(:candidate_servers) do
        spec.candidate_servers.collect do |s|
          address = Mongo::Address.new(s['address'])
          Mongo::Server.new(address, Mongo::Event::Listeners.new).tap do |server|
            allow(server).to receive(:average_round_trip_time).and_return(s['avg_rtt_ms'])
            allow(server).to receive(:tags).and_return(s['tag_sets'])
            allow(server).to receive(:secondary?).and_return(s['type'] == 'RSSecondary')
            allow(server).to receive(:primary?).and_return(s['type'] == 'RSPrimary')
          end
        end
      end

      let(:in_latency_window) do
        spec.in_latency_window.collect do |s|
          address = Mongo::Address.new(s['address'])
          Mongo::Server.new(address, Mongo::Event::Listeners.new).tap do |server|
            allow(server).to receive(:average_round_trip_time).and_return(s['avg_rtt_ms'])
            allow(server).to receive(:tags).and_return(s['tag_sets'])
          end
        end
      end

      let(:selector) do
        Mongo::ServerSelector.get({ :mode => spec.read_preference['mode'],
                                    :tag_sets => spec.read_preference['tag_sets'] },
                                    :server_selection_timeout => 1)
      end

      before do
        allow(cluster).to receive(:servers).and_return(candidate_servers)
      end

      it 'Finds the most suitable server in the latency window' do
        if in_latency_window.empty?
          if spec.read_preference['mode'] == 'Primary' && spec.read_preference['tag_sets']
            expect do
              selector.select_server(cluster)
            end.to raise_exception(Mongo::ServerSelector::InvalidServerPreference)
          else
            expect do
              selector.select_server(cluster)
            end.to raise_exception(Mongo::ServerSelector::NoServerAvailable)
          end
        else
          expect(in_latency_window).to include(selector.select_server(cluster))
        end
      end

      it 'Finds all suitable servers in the latency window' do
        if cluster.replica_set?
          if in_latency_window.empty?
            if spec.read_preference['mode'] == 'Primary' && spec.read_preference['tag_sets']
              expect do
                selector.select_server(cluster)
              end.to raise_exception(Mongo::ServerSelector::InvalidServerPreference)
            else
              expect do
                selector.select_server(cluster)
              end.to raise_exception(Mongo::ServerSelector::NoServerAvailable)
            end
          else
            expect(selector.send(:select, cluster.servers)).to eq(in_latency_window)
          end
        end        
      end
    end
  end
end

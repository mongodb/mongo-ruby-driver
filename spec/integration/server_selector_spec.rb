# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe 'Server selector' do
  require_no_linting

  let(:selector) { Mongo::ServerSelector::Primary.new }
  let(:client) { authorized_client }
  let(:cluster) { client.cluster }

  describe '#select_server' do
    # These tests operate on specific servers, and don't work in a multi
    # shard cluster where multiple servers are equally eligible
    require_no_multi_mongos

    let(:result) { selector.select_server(cluster) }

    it 'selects' do
      expect(result).to be_a(Mongo::Server)
    end

    context 'no servers in the cluster' do
      let(:client) { new_local_client_nmio([], server_selection_timeout: 2) }

      it 'raises NoServerAvailable with a message explaining the situation' do
        expect do
          result
        end.to raise_error(Mongo::Error::NoServerAvailable, "Cluster has no addresses, and therefore will never have a server")
      end

      it 'does not wait for server selection timeout' do
        start_time = Mongo::Utils.monotonic_time
        expect do
          result
        end.to raise_error(Mongo::Error::NoServerAvailable)
        time_passed = Mongo::Utils.monotonic_time - start_time
        expect(time_passed).to be < 1
      end
    end

    context 'client is closed' do
      context 'there is a known primary' do
        before do
          client.cluster.next_primary
          client.close
          expect(client.cluster.connected?).to be false
        end

        it 'returns the primary for BC reasons' do
          expect(result).to be_a(Mongo::Server)
        end
      end

      context 'there is no known primary' do
        require_topology :single, :replica_set, :sharded

        before do
          primary_server = client.cluster.next_primary
          client.close
          expect(client.cluster.connected?).to be false
          primary_server.unknown!
        end

        context 'non-lb' do
          require_topology :single, :replica_set, :sharded

          it 'raises NoServerAvailable with a message explaining the situation' do
            expect do
              result
            end.to raise_error(Mongo::Error::NoServerAvailable, /The cluster is disconnected \(client may have been closed\)/)
          end
        end

        context 'lb' do
          require_topology :load_balanced

          it 'returns the load balancer' do
            expect(result).to be_a(Mongo::Server)
            result.should be_load_balancer
          end
        end
      end
    end

    context 'monitoring thread is dead' do
      require_topology :single, :replica_set, :sharded

      before do
        client.cluster.servers.each do |server|
          server.monitor.instance_variable_get('@thread').kill
        end
        server = client.cluster.next_primary
        if server
          server.instance_variable_set('@description', Mongo::Server::Description.new({}))
        end
      end

      it 'raises NoServerAvailable with a message explaining the situation' do
        expect do
          result
        end.to raise_error(Mongo::Error::NoServerAvailable, /The following servers have dead monitor threads/)
      end
    end
  end
end

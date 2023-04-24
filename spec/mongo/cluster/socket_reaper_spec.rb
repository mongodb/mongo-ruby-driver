# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe Mongo::Cluster::SocketReaper do

  let(:cluster) do
    authorized_client.cluster
  end

  let(:reaper) do
    described_class.new(cluster)
  end

  describe '#initialize' do

    it 'takes a cluster as an argument' do
      expect(reaper).to be_a(described_class)
    end
  end

  describe '#execute' do

    before do
      # Ensure all servers are discovered
      cluster.servers_list.each do |server|
        server.scan!
      end

      # Stop the reaper that is attached to the cluster, since it
      # runs the same code we are running and can interfere with our assertions
      cluster.instance_variable_get('@periodic_executor').stop!
    end

    it 'calls close_idle_sockets on each connection pool in the cluster' do
      RSpec::Mocks.with_temporary_scope do
        cluster.servers.each do |s|
          expect(s.pool).to receive(:close_idle_sockets).and_call_original
        end

        reaper.execute
      end
    end
  end
end

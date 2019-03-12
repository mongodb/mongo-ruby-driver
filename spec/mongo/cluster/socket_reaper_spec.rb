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
      cluster.servers.each do |s|
        expect(s.pool).to receive(:close_idle_sockets).and_call_original
      end
    end

    it 'calls close_idle_sockets on each connection pool in the cluster' do
      reaper.execute
    end
  end
end

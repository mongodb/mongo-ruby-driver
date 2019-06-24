require 'spec_helper'

describe Mongo::Server::ConnectionPool do
  let(:options) { {max_pool_size: 2} }

  let(:server_options) do
    SpecConfig.instance.test_options.merge(options)
  end

  let(:address) do
    Mongo::Address.new(SpecConfig.instance.addresses.first)
  end

  let(:monitoring) do
    Mongo::Monitoring.new(monitoring: false)
  end

  let(:listeners) do
    Mongo::Event::Listeners.new
  end

  declare_topology_double

  let(:cluster) do
    double('cluster').tap do |cl|
      allow(cl).to receive(:topology).and_return(topology)
      allow(cl).to receive(:app_metadata).and_return(app_metadata)
      allow(cl).to receive(:options).and_return({})
      allow(cl).to receive(:update_cluster_time)
    end
  end

  let(:server) do
    Mongo::Server.new(address, cluster, monitoring, listeners, server_options)
  end

  let(:pool) do
    described_class.new(server)
  end

  describe '#initialize' do
    context 'when a min size is provided' do

      let(:pool) do
        described_class.new(server, :min_pool_size => 2)
      end

      it 'creates the pool with min pool size connections' do
        pool
        sleep 2

        expect(pool.size).to eq(2)
        expect(pool.available_count).to eq(2)
      end

      it 'does not use the same objects in the pool' do
        expect(pool.check_out).to_not equal(pool.check_out)
      end
    end
  end

  describe '#clear' do
    context 'when a min size is provided' do
      let(:pool) do
        described_class.new(server, :min_pool_size => 1)
      end

      it 'repopulates the pool periodically only up to min size' do
        pool 
        sleep 2

        expect(pool.size).to eq(1)

        pool.clear
        expect(pool.size).to eq(0)

        sleep 2
        expect(pool.size).to eq(1)

        sleep 2
        expect(pool.size).to eq(1)
      end
    end
  end
end

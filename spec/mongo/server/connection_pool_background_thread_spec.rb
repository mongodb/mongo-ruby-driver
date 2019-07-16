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
    register_server(
      Mongo::Server.new(address, cluster, monitoring, listeners,
        {monitoring_io: false}.update(server_options)
      ).tap do |server|
        allow(server).to receive(:description).and_return(ClusterConfig.instance.primary_description)
      end
    )
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
        sleep 0.1

        expect(pool.size).to eq(2)
        expect(pool.available_count).to eq(2)
      end

      it 'does not use the same objects in the pool' do
        expect(pool.check_out).to_not equal(pool.check_out)
      end
    end

    context 'when min size is zero' do
      let(:pool) do
        described_class.new(server)
      end

      it 'does not start the background thread' do
        pool
        sleep 0.1

        expect(pool.size).to eq(0)
        expect(pool.populator.running?).to be false
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

        sleep 0.1
        expect(pool.size).to eq(1)
        expect(pool.available_count).to eq(1)
        first_connection = pool.check_out
        pool.check_in(first_connection)

        pool.clear

        sleep 0.1
        expect(pool.size).to eq(1)
        expect(pool.available_count).to eq(1)
        second_connection = pool.check_out
        pool.check_in(second_connection)
        expect(second_connection).to_not eq(first_connection)

        # When populate is re-run, the pool size should not change
        pool.populate
        expect(pool.size).to eq(1)
        expect(pool.available_count).to eq(1)
        third_connection = pool.check_out
        expect(third_connection).to eq(second_connection)
      end
    end
  end

  describe '#check_in' do
    context 'when a min size is provided' do
      let(:pool) do
        described_class.new(server, :min_pool_size => 1)
      end

      it 'repopulates the pool after check_in of a closed connection' do
        pool

        sleep 0.1
        expect(pool.size).to eq(1)
        first_connection = pool.check_out
        first_connection.disconnect!
        expect(pool.size).to eq(1)

        pool.check_in(first_connection)

        sleep 0.1
        expect(pool.size).to eq(1)
        expect(pool.available_count).to eq(1)
        second_connection = pool.check_out
        expect(second_connection).to_not eq(first_connection)
      end
    end
  end

  describe '#check_out' do
    context 'when min size and idle time are provided' do

      let(:pool) do
        described_class.new(server, :min_pool_size => 2, :max_idle_time => 0.5)
      end

      it 'repopulates the pool after check_out empties idle connections' do
        pool

        first_connection = pool.check_out
        second_connection = pool.check_out

        first_connection.record_checkin!
        second_connection.record_checkin!

        pool.check_in(first_connection)
        pool.check_in(second_connection)

        expect(pool.size).to eq(2)

        # let both connections become idle
        sleep 0.5

        # check_out should discard first two connections, trigger in-flow
        # creation of a single connection, then wake up populate thread
        third_connection = pool.check_out
        expect(third_connection).to_not eq(first_connection)
        expect(third_connection).to_not eq(second_connection)

        # populate thread should create a new connection for the pool
        sleep 0.1
        expect(pool.size).to eq(2)
        fourth_connection = pool.check_out
        expect(fourth_connection).to_not eq(first_connection)
        expect(fourth_connection).to_not eq(second_connection)
        expect(fourth_connection).to_not eq(third_connection)
      end
    end
  end

  describe '#close' do
    context 'when min size is provided' do

      let(:pool) do
        described_class.new(server, :min_pool_size => 2)
      end

      it 'terminates and does not repopulate the pool after pool is closed' do
        pool

        sleep 0.1
        expect(pool.size).to eq(2)

        connection = pool.check_out

        expect(pool.size).to eq(2)
        pool.close(:force => true)

        expect(pool.closed?).to be true
        expect(pool.instance_variable_get('@available_connections').empty?).to be true
        expect(pool.instance_variable_get('@checked_out_connections').empty?).to be true

        # populate thread should terminate
        sleep 0.1
        expect(pool.populator.running?).to be false
        expect(pool.closed?).to be true

        # running populate should not change state of pool
        pool.populate
        expect(pool.instance_variable_get('@available_connections').empty?).to be true
        expect(pool.instance_variable_get('@checked_out_connections').empty?).to be true
      end
    end
  end

  describe '#close_idle_sockets' do
    context 'when min size and idle time are provided' do
      let(:pool) do
        described_class.new(server, :min_pool_size => 1, :max_idle_time => 0.5)
      end

      it 'repopulates pool after sockets are closes' do
        pool

        sleep 0.1
        expect(pool.size).to eq(1)

        connection = pool.check_out
        connection.record_checkin!
        pool.check_in(connection)

        # let the connection become idle
        sleep 0.5

        # close idle_sockets should trigger populate
        pool.close_idle_sockets

        sleep 0.1
        expect(pool.size).to eq(1)
        expect(pool.check_out).not_to eq(connection)
      end
    end
  end

  describe 'when forking is enabled' do
    only_mri

    context 'when min size is provided' do
      min_server_version '2.8'

      it 'populates the parent and child pools' do
        client = ClientRegistry.instance.new_local_client([SpecConfig.instance.addresses.first],
          server_options.merge(min_pool_size: 2))
        # let pool populate
        sleep 0.1

        server = client.cluster.next_primary
        pool = server.pool
        expect(pool.size).to eq(2)

        fork do
          # follow forking guidance
          client.close(true)
          client.reconnect
          # let pool populate
          sleep 0.1

          server = client.cluster.next_primary
          pool = server.pool
          expect(pool.size).to eq(2)
        end
      end
    end
  end
end

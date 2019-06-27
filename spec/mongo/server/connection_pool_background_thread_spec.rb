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
        expect(pool.instance_variable_get('@populator').is_running?).to be false
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

        pool.clear
        expect(pool.size).to eq(0) # todo what if bg thread is scheduled before?

        sleep 0.1
        expect(pool.size).to eq(1)

        # even if populate is re-run, size does not change
        pool.populate
        expect(pool.size).to eq(1)
      end
    end
  end

  describe '#check_in' do
    context 'when a min size is provided' do
      let(:pool) do
        described_class.new(server, :min_pool_size => 1)
      end

      it 'repopulates the pool after check_in of closed connection' do
        pool

        sleep 0.1
        connection = pool.check_out
        expect(pool.size).to eq(1)

        connection.disconnect!
        expect(pool.size).to eq(1)

        pool.check_in(connection)
        expect(pool.size).to eq(0) # todo what if bg thread is scheduled before?

        sleep 0.1
        expect(pool.size).to eq(1)
      end
    end
  end

  describe '#check_out' do
    context 'when min size and idle time are provided' do

      let(:pool) do
        described_class.new(server, :min_pool_size => 2, :max_idle_time => 1)
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
        sleep 1

        # should trigger in-flow creation of a single connection,
        # then wake up populate thread
        third_connection = pool.check_out
        expect(third_connection).to_not eq(first_connection)
        expect(third_connection).to_not eq(second_connection)

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

      it 'does not repopulate the pool after pool is closed' do
        pool

        sleep 0.1
        expect(pool.size).to eq(2)

        connection = pool.check_out

        expect(pool.size).to eq(2)
        pool.close(:force => true)

        expect(pool.closed?).to be true
        expect(pool.instance_variable_get('@available_connections').empty?).to be true
        expect(pool.instance_variable_get('@checked_out_connections').empty?).to be true

        # even if populate is re-run, the state of the closed pool does not change
        sleep 0.1
        pool.populate
        expect(pool.closed?).to be true
        expect(pool.instance_variable_get('@available_connections').empty?).to be true
        expect(pool.instance_variable_get('@checked_out_connections').empty?).to be true
        expect(pool.instance_variable_get('@populator').is_running?).to be false
      end
    end
  end

  describe '#close_idle_sockets' do
    context 'when min size and idle time are provided' do
      let(:pool) do
        described_class.new(server, :min_pool_size => 1, :max_idle_time => 1)
      end

      it 'repopulates pool after sockets are closes' do
        pool

        sleep 0.1
        expect(pool.size).to eq(1)

        connection = pool.check_out
        connection.record_checkin!
        pool.check_in(connection)

        # let the connection become idle
        sleep 1

        # force close idle_sockets so it triggers populate,
        # and it is unlikely to be because of bg thread timeout
        pool.close_idle_sockets
        expect(pool.size).to eq(0) # todo what if bg thread is scheduled before?

        # wait for populate to finish
        sleep 0.1
        expect(pool.size).to eq(1)
        expect(pool.check_out).not_to eq(connection)
      end
    end
  end

  # todo test not going over max size / interactions between
  # bg thread and in-flow checkout

  describe 'when forking is enabled' do
    only_mri

    context 'when min size is provided' do
      min_server_version '2.8'

      let (:client) do
        ClientRegistry.instance.new_local_client([SpecConfig.instance.addresses.first], SpecConfig.instance.test_options.merge( max_pool_size: 2, min_pool_size: 2 ))
      end

      it 'populates the parent and child pools' do
        server = client.cluster.next_primary
        pool = server.pool
        sleep 0.1
        expect(pool.size).to eq(2)

        fork do
          # follow forking guidance
          client.close
          client.reconnect
          server = client.cluster.next_primary
          pool = server.pool
          sleep 0.1
          expect(pool.size).to eq(2)
        end
      end
    end
  end
end

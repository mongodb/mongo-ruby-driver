# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe 'Connection pool populator integration' do
  let(:options) { {} }

  let(:server_options) do
    Mongo::Utils.shallow_symbolize_keys(Mongo::Client.canonicalize_ruby_options(
      SpecConfig.instance.all_test_options,
    )).update(options)
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

  let(:app_metadata) do
    Mongo::Server::AppMetadata.new(options)
  end

  let(:cluster) do
    double('cluster').tap do |cl|
      allow(cl).to receive(:topology).and_return(topology)
      allow(cl).to receive(:app_metadata).and_return(app_metadata)
      allow(cl).to receive(:options).and_return({})
      allow(cl).to receive(:update_cluster_time)
      allow(cl).to receive(:cluster_time).and_return(nil)
      allow(cl).to receive(:run_sdam_flow)
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
    server.pool
  end

  describe '#initialize' do
    context 'when a min size is provided' do

      let(:options) do
        { min_pool_size: 2, max_pool_size: 5 }
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

    context 'when min size is zero' do

      it 'does start the background thread' do
        pool
        sleep 2

        expect(pool.size).to eq(0)
        expect(pool.instance_variable_get('@populator')).to be_running
      end
    end
  end

  describe '#clear' do
    context 'when a min size is provided' do
      require_no_linting

       let(:options) do
        { min_pool_size: 1 }
      end

      it 'repopulates the pool periodically only up to min size' do
        pool.ready
        expect(pool.instance_variable_get('@populator')).to be_running

        sleep 2
        expect(pool.size).to eq(1)
        expect(pool.available_count).to eq(1)
        first_connection = pool.check_out
        pool.check_in(first_connection)

        RSpec::Mocks.with_temporary_scope do
          allow(pool.server).to receive(:unknown?).and_return(true)
          if server.load_balancer?
            pool.clear(service_id: first_connection.service_id)
          else
            pool.clear
          end
        end

        ::Utils.wait_for_condition(3) do
          pool.size == 0
        end
        expect(pool.size).to eq(0)

        pool.ready
        sleep 2
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
       let(:options) do
        { min_pool_size: 1 }
      end

      it 'repopulates the pool after check_in of a closed connection' do
        pool

        sleep 2
        expect(pool.size).to eq(1)
        first_connection = pool.check_out
        first_connection.disconnect!
        expect(pool.size).to eq(1)

        pool.check_in(first_connection)

        sleep 2
        expect(pool.size).to eq(1)
        expect(pool.available_count).to eq(1)
        second_connection = pool.check_out
        expect(second_connection).to_not eq(first_connection)
      end
    end
  end

  describe '#check_out' do
    context 'when min size and idle time are provided' do

      let(:options) do
        { max_pool_size: 2, min_pool_size: 2, max_idle_time: 0.5 }
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
        sleep 2
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

      let(:options) do
        { min_pool_size: 2, max_pool_size: 5 }
      end

      it 'terminates and does not repopulate the pool after pool is closed' do
        pool

        sleep 2
        expect(pool.size).to eq(2)

        connection = pool.check_out

        expect(pool.size).to eq(2)
        pool.close(force: true)

        expect(pool.closed?).to be true
        expect(pool.instance_variable_get('@available_connections').empty?).to be true
        expect(pool.instance_variable_get('@checked_out_connections').empty?).to be true

        # populate thread should terminate
        sleep 2
        expect(pool.instance_variable_get('@populator').running?).to be false
        expect(pool.closed?).to be true
      end
    end
  end

  describe '#close_idle_sockets' do
    context 'when min size and idle time are provided' do
      let(:options) do
        { min_pool_size: 1, max_idle_time: 0.5 }
      end

      it 'repopulates pool after sockets are closes' do
        pool

        sleep 2
        expect(pool.size).to eq(1)

        connection = pool.check_out
        connection.record_checkin!
        pool.check_in(connection)

        # let the connection become idle
        sleep 0.5

        # close idle_sockets should trigger populate
        pool.close_idle_sockets

        sleep 2
        expect(pool.size).to eq(1)
        expect(pool.check_out).not_to eq(connection)
      end
    end
  end

  describe '#populate' do
    let(:options) do
      { min_pool_size: 1 }
    end

    context 'when populate encounters a network error twice' do
      it 'retries once and does not stop the populator' do
        expect_any_instance_of(Mongo::Server::ConnectionPool).to \
          receive(:create_and_add_connection).twice.and_raise(Mongo::Error::SocketError)
        pool
        sleep 2
        expect(pool.populator).to be_running
      end
    end

    context 'when populate encounters a non-network error' do
      it 'does not retry and does not stop the populator' do
        expect_any_instance_of(Mongo::Server::ConnectionPool).to \
          receive(:create_and_add_connection).and_raise(Mongo::Error)
        pool
        sleep 2
        expect(pool.populator).to be_running
      end
    end
  end

  describe 'when forking is enabled' do
    require_mri

    context 'when min size is provided' do
      min_server_version '2.8'

      it 'populates the parent and child pools' do
        client = ClientRegistry.instance.new_local_client([SpecConfig.instance.addresses.first],
          server_options.merge(min_pool_size: 2, max_pool_size: 5))

        # force initialization of the pool
        client.cluster.servers.first.pool

        # let pool populate
        sleep 2
        server = client.cluster.next_primary
        pool = server.pool
        expect(pool.size).to eq(2)

        fork do
          # follow forking guidance
          client.close
          client.reconnect
          # let pool populate
          sleep 2

          server = client.cluster.next_primary
          pool = server.pool
          expect(pool.size).to eq(2)
        end
      end
    end
  end
end

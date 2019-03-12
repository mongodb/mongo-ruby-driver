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

      it 'creates the pool with no connections' do
        expect(pool.size).to eq(0)
        expect(pool.available_count).to eq(0)
      end

      it 'does not use the same objects in the pool' do
        expect(pool.check_out).to_not equal(pool.check_out)
      end
    end

    context 'when min size exceeds default max size' do

      let(:pool) do
        described_class.new(server, :min_pool_size => 10)
      end

      it 'sets max size to equal provided min size' do
        expect(pool.max_size).to eq(10)
      end
    end

    context 'when no min size is provided' do

      let(:pool) do
        described_class.new(server)
      end

      it 'creates the pool with no connections' do
        expect(pool.size).to eq(0)
        expect(pool.available_count).to eq(0)
      end
    end

    context 'sizes given as min_size and max_size' do

      let(:pool) do
        described_class.new(server, min_size: 3, max_size: 7)
      end

      it 'sets sizes correctly' do
        expect(pool.min_size).to eq(3)
        expect(pool.max_size).to eq(7)
      end
    end

    context 'sizes given as min_pool_size and max_pool_size' do

      let(:pool) do
        described_class.new(server, min_pool_size: 3, max_pool_size: 7)
      end

      it 'sets sizes correctly' do
        expect(pool.min_size).to eq(3)
        expect(pool.max_size).to eq(7)
      end
    end

    context 'timeout given as wait_timeout' do

      let(:pool) do
        described_class.new(server, wait_timeout: 4)
      end

      it 'sets wait timeout correctly' do
        expect(pool.wait_timeout).to eq(4)
      end
    end

    context 'timeout given as wait_queue_timeout' do

      let(:pool) do
        described_class.new(server, wait_queue_timeout: 4)
      end

      it 'sets wait timeout correctly' do
        expect(pool.wait_timeout).to eq(4)
      end
    end
  end

  describe '#max_size' do

    context 'when a max pool size option is provided' do

      let(:pool) do
        described_class.new(server, :max_pool_size => 3)
      end

      it 'returns the max size' do
        expect(pool.max_size).to eq(3)
      end
    end

    context 'when no pool size option is provided' do

      it 'returns the default size' do
        expect(pool.max_size).to eq(5)
      end
    end

    context 'when pool is closed' do
      before do
        pool.close
      end

      it 'returns max size' do
        expect(pool.max_size).to eq(5)
      end
    end
  end

  describe '#wait_timeout' do

    context 'when the wait timeout option is provided' do

      let(:pool) do
        described_class.new(server, :wait_queue_timeout => 3)
      end

      it 'returns the wait timeout' do
        expect(pool.wait_timeout).to eq(3)
      end
    end

    context 'when the wait timeout option is not provided' do

      it 'returns the default wait timeout' do
        expect(pool.wait_timeout).to eq(1)
      end
    end
  end

  describe '#size' do
    context 'pool without connections' do
      it 'is 0' do
        expect(pool.size).to eq(0)
      end
    end

    context 'pool with a checked out connection' do
      before do
        pool.check_out
      end

      it 'is 1' do
        expect(pool.size).to eq(1)
      end
    end

    context 'pool with an available connection' do
      before do
        connection = pool.check_out
        pool.check_in(connection)
      end

      it 'is 1' do
        expect(pool.size).to eq(1)
      end
    end

    context 'when pool is closed' do
      before do
        pool.close
      end

      it 'raises PoolClosedError' do
        expect do
          pool.size
        end.to raise_error(Mongo::Error::PoolClosedError)
      end
    end
  end

  describe '#available_count' do
    context 'pool without connections' do
      it 'is 0' do
        expect(pool.available_count).to eq(0)
      end
    end

    context 'pool with a checked out connection' do
      before do
        pool.check_out
      end

      it 'is 0' do
        expect(pool.available_count).to eq(0)
      end
    end

    context 'pool with an available connection' do
      before do
        connection = pool.check_out
        pool.check_in(connection)
      end

      it 'is 1' do
        expect(pool.available_count).to eq(1)
      end
    end

    context 'when pool is closed' do
      before do
        pool.close
      end

      it 'raises PoolClosedError' do
        expect do
          pool.available_count
        end.to raise_error(Mongo::Error::PoolClosedError)
      end
    end
  end

  describe '#closed?' do
    context 'pool is not closed' do
      it 'is false' do
        expect(pool.closed?).to be false
      end
    end

    context 'pool is closed' do
      before do
        pool.close
      end

      it 'is true' do
        expect(pool.closed?).to be true
      end
    end
  end

  describe '#check_in' do

    let!(:pool) do
      server.pool
    end

    after do
      expect(server).to receive(:pool).and_return(pool)
      server.disconnect!
    end

    let(:options) { {max_pool_size: 2} }

    let(:connection) do
      pool.check_out
    end

    context 'when a connection is checked out on the thread' do

      before do
        pool.check_in(connection)
      end

      it 'returns the connection to the pool' do
        expect(pool.size).to eq(1)
      end
    end

    context 'connection of the same generation as pool' do
      before do
        expect(pool.generation).to eq(connection.generation)
      end

      it 'adds the connection to the pool' do
        # connection is checked out
        expect(pool.available_count).to eq(0)
        expect(pool.size).to eq(1)
        pool.check_in(connection)
        # now connection is in the queue
        expect(pool.available_count).to eq(1)
        expect(pool.size).to eq(1)
        expect(pool.check_out).to eq(connection)
      end
    end

    shared_examples 'does not add connection to pool' do
      it 'disconnects connection and does not add connection to pool' do
        # connection was checked out
        expect(pool.available_count).to eq(0)
        expect(pool.size).to eq(1)
        expect(connection).to receive(:disconnect!)
        pool.check_in(connection)
        # connection is not added to the pool, and no replacement
        # connection has been created at this point
        expect(pool.available_count).to eq(0)
        expect(pool.size).to eq(0)
        expect(pool.check_out).not_to eq(connection)
      end
    end

    context 'connection of earlier generation than pool' do
      let(:connection) do
        pool.check_out.tap do |connection|
          expect(connection).to receive(:generation).at_least(:once).and_return(0)
          expect(connection).not_to receive(:record_checkin!)
        end
      end

      before do
        expect(connection.generation < pool.generation).to be true
      end

      it_behaves_like 'does not add connection to pool'
    end

    context 'connection of later generation than pool' do
      let(:connection) do
        pool.check_out.tap do |connection|
          expect(connection).to receive(:generation).at_least(:once).and_return(7)
          expect(connection).not_to receive(:record_checkin!)
        end
      end

      before do
        expect(connection.generation > pool.generation).to be true
      end

      it_behaves_like 'does not add connection to pool'
    end

    context 'when pool is closed' do
      let(:connection) { pool.check_out }

      before do
        connection
        pool.close
      end

      it 'closes connection' do
        expect(connection.closed?).to be false
        expect(pool.instance_variable_get('@available_connections').length).to eq(0)
        pool.check_in(connection)
        expect(connection.closed?).to be true
        expect(pool.instance_variable_get('@available_connections').length).to eq(0)
      end
    end
  end

  describe '#check_out' do

    let!(:pool) do
      server.pool
    end

    context 'when a connection is checked out on a different thread' do

      let!(:connection) do
        Thread.new { pool.check_out }.join
      end

      it 'returns a new connection' do
        expect(pool.check_out.address).to eq(server.address)
      end

      it 'does not return the same connection instance' do
        expect(pool.check_out).to_not eql(connection)
      end
    end

    context 'when connections are checked out and checked back in' do

      it 'pulls the connection from the front of the queue' do
        first = pool.check_out
        second = pool.check_out
        pool.check_in(second)
        pool.check_in(first)
        expect(pool.check_out).to be(first)
      end
    end

    context 'when there is an available connection which is stale' do
      let(:options) do
        {max_pool_size: 2, max_idle_time: 0.1}
      end

      let(:connection) do
        pool.check_out.tap do |connection|
          allow(connection).to receive(:generation).and_return(pool.generation)
          allow(connection).to receive(:record_checkin!).and_return(connection)
          expect(connection).to receive(:last_checkin).at_least(:once).and_return(Time.now - 10)
        end
      end

      before do
        pool.check_in(connection)
      end

      after do
        pool.close(force: true)
      end

      it 'closes stale connection and creates a new one' do
        expect(connection).to receive(:disconnect!)
        expect(Mongo::Server::Connection).to receive(:new).and_call_original
        pool.check_out
      end
    end

    context 'when there are no available connections' do

      let(:options) do
        {max_pool_size: 1}
      end

      context 'when the max size is not reached' do

        it 'creates a new connection' do
          expect(Mongo::Server::Connection).to receive(:new).once.and_call_original
          expect(pool.check_out).to be_a(Mongo::Server::Connection)
          expect(pool.size).to eq(1)
        end
      end

      context 'when the max size is reached' do

        it 'raises a timeout error' do
          expect(Mongo::Server::Connection).to receive(:new).once.and_call_original
          expect {
            pool.check_out
            pool.check_out
          }.to raise_error(Timeout::Error)
          expect(pool.size).to eq(1)
        end
      end
    end

    context 'when waiting for a connection to be checked in' do

      let!(:connection) { pool.check_out }

      before do
        allow(connection).to receive(:record_checkin!).and_return(connection)
        Thread.new do
          sleep(0.5)
          pool.check_in(connection)
        end.join
      end

      it 'returns the checked in connection' do
        expect(pool.check_out).to eq(connection)
      end
    end

    context 'when pool is closed' do
      before do
        pool.close
      end

      it 'raises PoolClosedError' do
        expect do
          pool.check_out
        end.to raise_error(Mongo::Error::PoolClosedError)
      end
    end
  end

  describe '#disconnect!' do

    def create_pool(min_pool_size)
      described_class.new(server, max_pool_size: 3, min_pool_size: min_pool_size).tap do |pool|
        # make pool be of size 2 so that it has enqueued connections
        # when told to disconnect
        c1 = pool.check_out
        c2 = pool.check_out
        allow(c1).to receive(:record_checkin!).and_return(c1)
        allow(c2).to receive(:record_checkin!).and_return(c2)
        pool.check_in(c1)
        pool.check_in(c2)
        expect(pool.size).to eq(2)
        expect(pool.available_count).to eq(2)
      end
    end

    shared_examples_for 'disconnects and removes all connections in the pool and bumps generation' do
      it 'disconnects and removes and bumps' do
        old_connections = []
        pool.instance_variable_get('@available_connections').each do |connection|
          expect(connection).to receive(:disconnect!)
          old_connections << connection
        end

        expect(pool.size).to eq(2)
        expect(pool.available_count).to eq(2)

        pool.disconnect!

        expect(pool.size).to eq(0)
        expect(pool.available_count).to eq(0)

        new_connection = pool.check_out
        expect(old_connections).not_to include(new_connection)
        expect(new_connection.generation).to eq(2)
      end
    end

    context 'min size is 0' do
      let(:pool) do
        create_pool(0)
      end

      it_behaves_like 'disconnects and removes all connections in the pool and bumps generation'
    end

    context 'min size is not 0' do
      let(:pool) do
        create_pool(1)
      end

      it_behaves_like 'disconnects and removes all connections in the pool and bumps generation'
    end

    context 'when pool is closed' do
      before do
        pool.close
      end

      it 'raises PoolClosedError' do
        expect do
          pool.disconnect!
        end.to raise_error(Mongo::Error::PoolClosedError)
      end
    end
  end

  describe '#close' do
    context 'when pool is not closed' do
      it 'closes the pool' do
        expect(pool).not_to be_closed

        pool.close

        expect(pool).to be_closed
      end
    end

    context 'when pool is closed' do
      before do
        pool.close
      end

      it 'is a no-op' do
        pool.close
        expect(pool).to be_closed
      end
    end
  end

  describe '#inspect' do
    let(:options) { {min_pool_size: 3, max_pool_size: 7, wait_timeout: 9, wait_queue_timeout: 9} }

    let!(:pool) do
      server.pool
    end

    after do
      expect(server).to receive(:pool).and_return(pool)
      server.disconnect!
    end

    it 'includes the object id' do
      expect(pool.inspect).to include(pool.object_id.to_s)
    end

    it 'includes the min size' do
      expect(pool.inspect).to include('min_size=3')
    end

    it 'includes the max size' do
      expect(pool.inspect).to include('max_size=7')
    end

    it 'includes the wait timeout' do
      expect(pool.inspect).to include('wait_timeout=9')
    end

    it 'includes the current size' do
      expect(pool.inspect).to include('current_size=0')
    end

=begin obsolete
    it 'includes the queue inspection' do
      expect(pool.inspect).to include(pool.__send__(:queue).inspect)
    end
=end

    it 'indicates the pool is not closed' do
      expect(pool.inspect).not_to include('closed')
    end

    context 'when pool is closed' do
      before do
        pool.close
      end

      it 'returns inspection string' do
        expect(pool.inspect).to include('min_size=')
      end

      it 'indicates the pool is closed' do
        expect(pool.inspect).to include('closed')
      end
    end
  end

  describe '#with_connection' do

    let!(:pool) do
      server.pool
    end

    context 'when a connection cannot be checked out' do

      it 'does not add the connection to the pool' do
        pending 'Re-enable when connections are connected prior to being returned from check_out method'

        allow(pool).to receive(:check_out).and_raise(Mongo::Error::SocketError)
        pool.with_connection { |c| c }

        expect(pool.size).to eq(0)
      end
    end

    context 'when pool is closed' do
      before do
        pool.close
      end

      it 'raises PoolClosedError' do
        expect do
          pool.with_connection { |c| c }
        end.to raise_error(Mongo::Error::PoolClosedError)
      end
    end
  end

  context 'when the connection does not finish authenticating before the thread is killed' do

    let!(:pool) do
      server.pool
    end

    let(:server_options) do
      { user: SpecConfig.instance.root_user.name, password: SpecConfig.instance.root_user.password }.merge(SpecConfig.instance.test_options).merge(max_pool_size: 1)
    end

    before do
      pool
      ClientRegistry.instance.close_all_clients
    end

    it 'creates a new connection' do
      invoked = nil

      t = Thread.new do
        # Kill the thread when it's authenticating
        expect(Mongo::Auth).to receive(:get) do
          if Thread.current != t
            raise 'Auth invoked on unexpected thread'
          end
          invoked = true
          t.kill
          raise 'Should not get here'
        end
        pool.with_connection do |c|
          c.send(:ensure_connected) { |socket| socket }
        end
      end
      t.join

      #expect(Mongo::Auth).to receive(:get).and_call_original
      expect(pool.check_out).to be_a(Mongo::Server::Connection)
      expect(invoked).to be true
    end
  end

  describe '#close_idle_sockets' do

    let!(:pool) do
      server.pool
    end

    context 'when there is a max_idle_time specified' do

      let(:options) do
        {max_pool_size: 2, max_idle_time: 0.5}
      end

      after do
        Timecop.return
      end

=begin obsolete
      context 'when the connections have not been checked out' do

        before do
          queue.each do |conn|
            expect(conn).not_to receive(:disconnect!)
          end
          sleep(0.5)
          pool.close_idle_sockets
        end

        it 'does not close any sockets' do
          expect(queue.none? { |c| c.connected? }).to be(true)
        end
      end
=end

      context 'when connections have been checked out and returned to the pool' do

        context 'when min size is 0' do

          let(:options) do
            {max_pool_size: 2, min_pool_size: 0, max_idle_time: 0.5}
          end

          before do
            c1 = pool.check_out
            c2 = pool.check_out
            pool.check_in(c1)
            pool.check_in(c2)
            sleep(0.5)
            expect(c1).to receive(:disconnect!).and_call_original
            expect(c2).to receive(:disconnect!).and_call_original
            pool.close_idle_sockets
          end

          it 'closes all idle sockets' do
            expect(pool.size).to be(0)
          end
        end

        context 'when min size is > 0' do
          context 'when more than the number of min_size are checked out' do
            let(:options) do
              {max_pool_size: 5, min_pool_size: 3, max_idle_time: 0.5}
            end

            it 'closes and removes connections with idle sockets and does not connect new ones' do
              first = pool.check_out
              second = pool.check_out
              third = pool.check_out
              fourth = pool.check_out
              fifth = pool.check_out

              pool.check_in(fifth)

              expect(fifth).to receive(:disconnect!).and_call_original
              expect(fifth).not_to receive(:connect!)

              Timecop.travel(Time.now + 1)
              expect(pool.size).to be(5)
              expect(pool.available_count).to be(1)
              pool.close_idle_sockets

              expect(pool.size).to be(4)
              expect(pool.available_count).to be(0)
              expect(fifth.connected?).to be(false)
            end
          end

          context 'when between 0 and min_size number of connections are checked out' do

            let(:options) do
              {max_pool_size: 5, min_pool_size: 3, max_idle_time: 0.5}
            end

            it 'closes and removes connections with idle sockets and does not connect new ones' do
              first = pool.check_out
              second = pool.check_out
              third = pool.check_out
              fourth = pool.check_out
              fifth = pool.check_out

              pool.check_in(third)
              pool.check_in(fourth)
              pool.check_in(fifth)


              expect(third).to receive(:disconnect!).and_call_original
              expect(third).not_to receive(:connect!)

              expect(fourth).to receive(:disconnect!).and_call_original
              expect(fourth).not_to receive(:connect!)

              expect(fifth).to receive(:disconnect!).and_call_original
              expect(fifth).not_to receive(:connect!).and_call_original

              Timecop.travel(Time.now + 1)
              expect(pool.size).to be(5)
              expect(pool.available_count).to be(3)
              pool.close_idle_sockets

              expect(pool.size).to be(2)
              expect(pool.available_count).to be(0)

              expect(third.connected?).to be(false)
              expect(fourth.connected?).to be(false)
              expect(fifth.connected?).to be(false)
            end
          end
        end
      end
    end

    context 'when available connections include idle and non-idle ones' do
      let(:pool) do
        described_class.new(server, max_pool_size: 2, max_idle_time: 0.5)
      end

      let(:connection) do
        pool.check_out.tap do |con|
          allow(con).to receive(:disconnect!)
        end
      end

      it 'disconnects all expired and only expired connections' do
        c1 = pool.check_out
        expect(c1).to receive(:disconnect!)
        c2 = pool.check_out
        expect(c2).not_to receive(:disconnect!)

        pool.check_in(c1)
        Timecop.travel(Time.now + 1)
        pool.check_in(c2)

        expect(pool.size).to eq(2)
        expect(pool.available_count).to eq(2)

        expect(c1).not_to receive(:connect!)
        expect(c2).not_to receive(:connect!)

        pool.close_idle_sockets

        expect(pool.size).to eq(1)
        expect(pool.available_count).to eq(1)
      end
    end

    context 'when there is no max_idle_time specified' do

      let(:connection) do
        conn = pool.check_out
        conn.connect!
        pool.check_in(conn)
        conn
      end

      before do
        expect(connection).not_to receive(:disconnect!)
        pool.close_idle_sockets
      end

      it 'does not close any sockets' do
        expect(connection.connected?).to be(true)
      end
    end
  end
end

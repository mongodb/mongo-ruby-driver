require 'spec_helper'

describe Mongo::Server::ConnectionPool do

  let(:options) do
    SpecConfig.instance.test_options.merge(max_pool_size: 2)
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
    end
  end

  describe '#checkin' do

    let(:server) do
      Mongo::Server.new(address, cluster, monitoring, listeners, options)
    end

    let!(:pool) do
      server.pool
    end

    after do
      expect(server).to receive(:pool).and_return(pool)
      server.disconnect!
    end

    context 'when a connection is checked out on the thread' do

      let!(:connection) do
        pool.checkout
      end

      before do
        pool.checkin(connection)
      end

      let(:queue) do
        pool.send(:queue).queue
      end

      it 'returns the connection to the queue' do
        expect(queue.size).to eq(1)
      end
    end
  end

  describe '#checkout' do

    let(:server) do
      Mongo::Server.new(address, cluster, monitoring, listeners, options)
    end

    let!(:pool) do
      server.pool
    end

    context 'when no connection is checked out on the same thread' do

      let!(:connection) do
        pool.checkout
      end

      it 'returns a new connection' do
        expect(connection.address).to eq(server.address)
      end
    end

    context 'when a connection is checked out on the same thread' do

      before do
        pool.checkout
      end

      it 'returns the threads connection' do
        expect(pool.checkout.address).to eq(server.address)
      end
    end

    context 'when a connection is checked out on a different thread' do

      let!(:connection) do
        Thread.new { pool.checkout }.join
      end

      it 'returns a new connection' do
        expect(pool.checkout.address).to eq(server.address)
      end

      it 'does not return the same connection instance' do
        expect(pool.checkout).to_not eql(connection)
      end
    end

    context 'when connections are checked out and checked back in' do

      it 'pulls the connection from the front of the queue' do
        first = pool.checkout
        second = pool.checkout
        pool.checkin(second)
        pool.checkin(first)
        expect(pool.checkout).to be(first)
      end
    end
  end

  describe '#disconnect!' do

    let(:server) do
      Mongo::Server.new(address, cluster, monitoring, listeners, options)
    end

    let!(:pool) do
      server.pool
    end

    it 'disconnects the queue' do
      expect(server).to receive(:pool).and_return(pool)
      expect(pool.send(:queue)).to receive(:disconnect!).once.and_call_original
      server.disconnect!
    end
  end

  describe '#inspect' do

    let(:server) do
      Mongo::Server.new(address, cluster, monitoring, listeners, options)
    end

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

    it 'includes the queue inspection' do
      expect(pool.inspect).to include(pool.__send__(:queue).inspect)
    end
  end

  describe '#with_connection' do

    let(:server) do
      Mongo::Server.new(address, cluster, monitoring, listeners, options)
    end

    let!(:pool) do
      server.pool
    end

    context 'when a connection cannot be checked out' do

      before do
        allow(pool).to receive(:checkout).and_return(nil)
        pool.with_connection { |c| c }
      end

      let(:queue) do
        pool.send(:queue).queue
      end

      it 'does not add the connection to the pool' do
        expect(queue.size).to eq(1)
      end
    end
  end

  context 'when the connection does not finish authenticating before the thread is killed' do

    let(:server) do
      Mongo::Server.new(address, cluster, monitoring, listeners, options)
    end

    let!(:pool) do
      server.pool
    end

    let(:options) do
      { user: SpecConfig.instance.root_user.name, password: SpecConfig.instance.root_user.password }.merge(SpecConfig.instance.test_options).merge(max_pool_size: 1)
    end

    before do
     t = Thread.new {
        # Kill the thread when it's authenticating
        allow(Mongo::Auth).to receive(:get) { t.kill && !t.alive? }
        pool.with_connection { |c| c.send(:ensure_connected) { |socket| socket } }
      }
      t.join
    end

    it 'disconnects the socket' do
      expect(pool.checkout.send(:socket)).to be_nil
    end
  end

  describe '#close_stale_sockets!' do

    let(:server) do
      Mongo::Server.new(address, authorized_client.cluster, monitoring, listeners, options)
    end

    let!(:pool) do
      server.pool
    end

    let(:queue) do
      pool.instance_variable_get(:@queue).queue
    end

    context 'when there is a max_idle_time specified' do

      let(:options) do
        SpecConfig.instance.test_options.merge(max_pool_size: 2, max_idle_time: 0.5)
      end

      context 'when the connections have not been checked out' do

        before do
          queue.each do |conn|
            expect(conn).not_to receive(:disconnect!)
          end
          sleep(0.5)
          pool.close_stale_sockets!
        end

        it 'does not close any sockets' do
          expect(queue.none? { |c| c.connected? }).to be(true)
        end
      end

      context 'when the sockets have already been checked out and returned to the pool' do

        context 'when min size is 0' do

          let(:options) do
            SpecConfig.instance.test_options.merge(max_pool_size: 2, min_pool_size: 0, max_idle_time: 0.5)
          end

          before do
            queue.each do |conn|
              expect(conn).to receive(:disconnect!).and_call_original
            end
            pool.checkin(pool.checkout)
            pool.checkin(pool.checkout)
            sleep(0.5)
            pool.close_stale_sockets!
          end

          it 'closes all stale sockets' do
            expect(queue.all? { |c| !c.connected? }).to be(true)
          end
        end

        context 'when min size is > 0' do
          after do
            Timecop.return
          end

          context 'when more than the number of min_size are checked out' do
            let(:options) do
              SpecConfig.instance.test_options.merge(max_pool_size: 5, min_pool_size: 3, max_idle_time: 0.5)
            end

            it 'closes and removes connections with stale sockets and does not connect new ones' do
              first = pool.checkout
              second = pool.checkout
              third = pool.checkout
              fourth = pool.checkout
              fifth = pool.checkout

              pool.checkin(fifth)

              expect(fifth).to receive(:disconnect!).and_call_original
              expect(fifth).not_to receive(:connect!)

              Timecop.travel(Time.now + 1)
              expect(queue.length).to be(1)
              pool.close_stale_sockets!

              expect(pool.send(:queue).pool_size).to be(4)
              expect(pool.send(:queue).queue_size).to be(0)
              expect(queue.length).to be(0)
              expect(fifth.connected?).to be(false)
            end
          end

          context 'when between 0 and min_size number of connections are checked out' do

            let(:options) do
              SpecConfig.instance.test_options.merge(max_pool_size: 5, min_pool_size: 3, max_idle_time: 0.5)
            end

            it 'closes and removes connections with stale sockets and does not connect new ones' do
              first = pool.checkout
              second = pool.checkout
              third = pool.checkout
              fourth = pool.checkout
              fifth = pool.checkout

              pool.checkin(third)
              pool.checkin(fourth)
              pool.checkin(fifth)


              expect(third).to receive(:disconnect!).and_call_original
              expect(third).not_to receive(:connect!)

              expect(fourth).to receive(:disconnect!).and_call_original
              expect(fourth).not_to receive(:connect!)

              expect(fifth).to receive(:disconnect!).and_call_original
              expect(fifth).not_to receive(:connect!).and_call_original

              Timecop.travel(Time.now + 1)
              expect(queue.length).to be(3)
              pool.close_stale_sockets!

              expect(pool.send(:queue).pool_size).to be(2)
              expect(pool.send(:queue).queue_size).to be(0)
              expect(queue.length).to be(0)

              expect(third.connected?).to be(false)
              expect(fourth.connected?).to be(false)
              expect(fifth.connected?).to be(false)
            end
          end
        end
      end
    end

    context 'when there is no max_idle_time specified' do

      let(:connection) do
        conn = pool.checkout
        conn.connect!
        pool.checkin(conn)
        conn
      end

      before do
        expect(connection).not_to receive(:disconnect!)
        pool.close_stale_sockets!
      end

      it 'does not close any sockets' do
        expect(connection.connected?).to be(true)
      end
    end
  end
end

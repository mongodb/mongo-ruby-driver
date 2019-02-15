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

  let(:server) do
    Mongo::Server.new(address, cluster, monitoring, listeners, options)
  end

  describe '#checkin' do

    let!(:pool) do
      described_class.get(server)
    end

    after do
      expect(cluster).to receive(:pool).with(server).and_return(pool)
      server.disconnect!
    end

    context 'when a connection is checked out on the thread' do

      let!(:connection) do
        pool.checkout
      end

      before do
        pool.checkin(connection)
      end

      let(:stack) do
        pool.send(:connections).connections
      end

      it 'returns the connection to the stack' do
        expect(stack.size).to eq(1)
      end
    end
  end

  describe '#checkout' do

    let(:server) do
      Mongo::Server.new(address, cluster, monitoring, listeners, options)
    end

    let!(:pool) do
      described_class.get(server)
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

      it 'pulls the connection from the front of the stack' do
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
      described_class.get(server)
    end

    it 'disconnects the stack' do
      expect(cluster).to receive(:pool).with(server).and_return(pool)
      expect(pool.send(:connections)).to receive(:close!).once.and_call_original
      server.disconnect!
    end
  end

  describe '.get' do

    let(:server) do
      Mongo::Server.new(address, cluster, monitoring, listeners, options)
    end

    let!(:pool) do
      described_class.get(server)
    end

    after do
      expect(cluster).to receive(:pool).with(server).and_return(pool)
      server.disconnect!
    end

    it 'returns the pool for the server' do
      expect(pool).to_not be_nil
    end
  end

  describe '#inspect' do

    let(:server) do
      Mongo::Server.new(address, cluster, monitoring, listeners, options)
    end

    let!(:pool) do
      described_class.get(server)
    end

    after do
      expect(cluster).to receive(:pool).with(server).and_return(pool)
      server.disconnect!
    end

    it 'includes the object id' do
      expect(pool.inspect).to include(pool.object_id.to_s)
    end

    it 'includes the wait timeout' do
      expect(pool.inspect).to include('wait_timeout=2')
    end

    it 'includes the stack inspection' do
      expect(pool.inspect).to include(pool.__send__(:connections).inspect)
    end
  end

  describe '#with_connection' do

    let(:server) do
      Mongo::Server.new(address, cluster, monitoring, listeners, options)
    end

    let!(:pool) do
      described_class.get(server)
    end

    context 'when a connection cannot be checked out and connected' do

      let(:options) do
        SpecConfig.instance.test_options.merge(max_pool_size: 2, min_pool_size: 1)
      end

      before do
        allow(pool).to receive(:checkout).and_raise(Exception)

        begin
          pool.with_connection { |c| c }
        rescue Exception
        end
      end

      let(:stack) do
        pool.send(:connections).connections
      end

      it 'does not add the connection to the pool' do
        expect(stack.size).to eq(1)
      end
    end
  end

  context 'when the connection does not finish authenticating before the thread is killed' do

    let(:server) do
      Mongo::Server.new(address, cluster, monitoring, listeners, options)
    end

    let!(:pool) do
      described_class.get(server)
    end

    let(:options) do
      { user: SpecConfig.instance.root_user.name, password: SpecConfig.instance.root_user.password }.merge(SpecConfig.instance.test_options).merge(max_pool_size: 1)
    end

    let(:connection) do
      pool.checkout
    end

    before do
     t = Thread.new {
        # Kill the thread when it's authenticating
        allow(Mongo::Auth).to receive(:get) { t.kill && !t.alive? }
        connection.send(:ensure_connected) { |socket| socket }
      }
      t.join
    end

    it 'disconnects the socket' do
      expect(connection.send(:socket)).to be_nil
    end
  end

  describe '#close_stale_sockets!' do

    let(:server) do
      Mongo::Server.new(address, authorized_client.cluster, monitoring, listeners, options)
    end

    let!(:pool) do
      described_class.get(server)
    end

    let(:stack) do
      pool.instance_variable_get(:@connections).connections
    end

    context 'when there is a max_idle_time specified' do

      let(:options) do
        SpecConfig.instance.test_options.merge(max_pool_size: 2, max_idle_time: 0.5)
      end

      context 'when the connections have not been checked out' do

        before do
          stack.each do |conn|
            expect(conn).not_to receive(:disconnect!)
          end
          sleep(0.5)
          pool.close_stale_sockets!
        end

        it 'does not close any sockets' do
          expect(stack.none? { |c| c.connected? }).to be(true)
        end
      end

      context 'when the sockets have already been checked out and returned to the pool' do

        context 'when min size is 0' do

          let(:options) do
            SpecConfig.instance.test_options.merge(max_pool_size: 2, min_pool_size: 0, max_idle_time: 0.5)
          end

          before do
            stack.each do |conn|
              expect(conn).to receive(:disconnect!).and_call_original
            end
            pool.checkin(pool.checkout)
            pool.checkin(pool.checkout)
            sleep(0.5)
            pool.close_stale_sockets!
          end

          it 'closes all stale sockets' do
            expect(stack.all? { |c| !c.connected? }).to be(true)
          end
        end

        context 'when min size is > 0' do

          context 'when more than the number of min_size are checked out' do

            let(:options) do
              SpecConfig.instance.test_options.merge(max_pool_size: 5, min_pool_size: 3, max_idle_time: 0.5)
            end

            before do
              first = pool.checkout
              second = pool.checkout
              third = pool.checkout
              fourth = pool.checkout
              fifth = pool.checkout

              pool.checkin(fifth)

              expect(fifth).to receive(:disconnect!).and_call_original
              expect(fifth).not_to receive(:connect!)

              sleep(0.5)
              pool.close_stale_sockets!
            end

            it 'closes all stale sockets and does not connect new ones' do
              expect(stack.size).to eq(0)
            end
          end

          context 'when between 0 and min_size number of connections are checked out' do

            let(:options) do
              SpecConfig.instance.test_options.merge(max_pool_size: 5, min_pool_size: 3, max_idle_time: 0.5)
            end

            before do
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
              expect(fifth).not_to receive(:connect!)

              sleep(0.5)
              pool.close_stale_sockets!
            end

            it 'closes all stale sockets and does not connect new ones' do
              expect(stack.size).to be(0)
            end
          end

          context 'when a stale connection is unsuccessfully reconnected' do

            let(:options) do
              SpecConfig.instance.test_options.merge(max_pool_size: 5, min_pool_size: 3, max_idle_time: 0.5)
            end

            before do
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
              allow(fifth).to receive(:connect!).and_raise(Mongo::Error::SocketError)

              sleep(0.5)
              pool.close_stale_sockets!
            end

            it 'is removed from the pool' do
              expect(stack.size).to be(0)
            end
          end

          context 'when exactly the min_size number of connections is checked out' do

            let(:options) do
              SpecConfig.instance.test_options.merge(max_pool_size: 5, min_pool_size: 3, max_idle_time: 0.5)
            end

            before do
              first = pool.checkout
              second = pool.checkout
              third = pool.checkout
              fourth = pool.checkout
              fifth = pool.checkout

              pool.checkin(fourth)
              pool.checkin(fifth)

              expect(fourth).to receive(:disconnect!).and_call_original
              expect(fourth).not_to receive(:connect!)

              expect(fifth).to receive(:disconnect!).and_call_original
              expect(fifth).not_to receive(:connect!)

              sleep(0.5)
              pool.close_stale_sockets!
            end

            it 'closes all stale sockets and does not connect new ones' do
              expect(stack.size).to be(0)
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

  describe '#wait_timeout' do

    context 'when the wait timeout option is provided' do

      let(:options) do
        { wait_queue_timeout: 3 }
      end

      let(:pool) do
        described_class.new(server) { Mongo::Server::Connection.new(server) }
      end

      it 'returns the wait timeout' do
        expect(pool.wait_timeout).to eq(3)
      end
    end

    context 'when the wait timeout option is not provided' do

      let(:options) do
        {}
      end

      let(:pool) do
        described_class.new(server) { Mongo::Server::Connection.new(server) }
      end

      it 'returns the default wait timeout' do
        expect(pool.wait_timeout).to eq(1)
      end
    end
  end
end

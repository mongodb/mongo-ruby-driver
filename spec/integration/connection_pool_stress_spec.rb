require 'spec_helper'

describe 'Connection pool stress test' do
  before(:all) do
    ClientRegistry.instance.close_all_clients
  end

  let(:options) do
    { max_pool_size: 5, min_pool_size: 3 }
  end

  let(:thread_count) { 5 }

  let(:documents) do
    [].tap do |documents|
      100.times do |i|
        documents << { a: i}
      end
    end
  end

  let(:threads) do
    [].tap do |threads|
      thread_count.times do |i|
        threads << Thread.new do
          10.times do |j|
            collection.find(a: i+j)
            sleep 0.5
            collection.find(a: i+j)
          end
        end
      end
    end
  end

  let(:client) do
    authorized_client.with(options.merge(monitoring: true))
  end

  let!(:collection) do
    client[authorized_collection.name].tap do |collection|
      collection.drop
      collection.insert_many(documents)
    end
  end

  after do
    client.close(true)
  end

  shared_examples_for 'does not raise error' do
    it 'does not raise error' do
      threads

      expect {
        threads.collect { |t| t.join }
      }.not_to raise_error
    end
  end

  describe 'min pool size less than max, fixed thread count' do
    context 'min pool size 0, max pool size 5' do
      let(:options) do
        { max_pool_size: 5, min_pool_size: 0 }
      end
      let(:thread_count) { 7 }

      it_behaves_like 'does not raise error'
    end

    context 'min pool size 1, max pool size 5' do
      let(:options) do
        { max_pool_size: 5, min_pool_size: 1 }
      end
      let(:thread_count) { 7 }

      it_behaves_like 'does not raise error'
    end

    context 'min pool size 2, max pool size 5' do
      let(:options) do
        { max_pool_size: 5, min_pool_size: 2 }
      end
      let(:thread_count) { 7 }

      it_behaves_like 'does not raise error'
    end

    context 'min pool size 3, max pool size 5' do
      let(:options) do
        { max_pool_size: 5, min_pool_size: 3 }
      end
      let(:thread_count) { 7 }

      it_behaves_like 'does not raise error'
    end

    context 'min pool size 4, max pool size 5' do
      let(:options) do
        { max_pool_size: 5, min_pool_size: 4 }
      end
      let(:thread_count) { 7 }

      it_behaves_like 'does not raise error'
    end
  end

  describe 'min pool size equal to max' do
    context 'thread count greater than max pool size' do
      let(:options) do
        { max_pool_size: 5, min_pool_size: 5 }
      end
      let(:thread_count) { 7 }

      it_behaves_like 'does not raise error'
    end

    context 'thread count equal to max pool size' do
      let(:options) do
        { max_pool_size: 5, min_pool_size: 5 }
      end
      let(:thread_count) { 5 }

      it_behaves_like 'does not raise error'
    end
  end

  describe 'thread count greater than max pool size' do
    context '6 threads, max pool size 5' do
      let(:options) do
        { max_pool_size: 5, min_pool_size: 3 }
      end
      let(:thread_count) { 6 }

      it_behaves_like 'does not raise error'
    end

    context '7 threads, max pool size 5' do
      let(:options) do
        { max_pool_size: 5, min_pool_size: 3 }
      end
      let(:thread_count) { 7 }

      it_behaves_like 'does not raise error'
    end

    context '8 threads, max pool size 5' do
      let(:options) do
        { max_pool_size: 5, min_pool_size: 3 }
      end
      let(:thread_count) { 8 }

      it_behaves_like 'does not raise error'
    end

    context '10 threads, max pool size 5' do
      let(:options) do
        { max_pool_size: 5, min_pool_size: 3 }
      end
      let(:thread_count) { 10 }

      it_behaves_like 'does not raise error'
    end

    context '15 threads, max pool size 5' do
      let(:options) do
        { max_pool_size: 5, min_pool_size: 3 }
      end
      let(:thread_count) { 15 }

      it_behaves_like 'does not raise error'
    end

    context '20 threads, max pool size 5' do
      let(:options) do
        { max_pool_size: 5, min_pool_size: 3 }
      end
      let(:thread_count) { 20 }

      it_behaves_like 'does not raise error'
    end
  end

  describe 'when primary pool is disconnected' do
    let(:threads) do
      threads = []

      # thread that performs operations
      threads << Thread.new do
        10.times do |j|
          collection.find(a: j)
          sleep 0.5
          collection.find(a: j)
        end
      end

      # thread that disconnects primary's pool
      threads << Thread.new do
        sleep 0.2
        server = client.cluster.next_primary
        server.pool.disconnect!
      end
    end

    context 'primary disconnected' do
      it_behaves_like 'does not raise error'
    end
  end

  describe 'when all pools are disconnected' do
    let(:threads) do
      threads = []

      # thread that performs operations
      threads << Thread.new do
        10.times do |j|
          collection.find(a: j)
          sleep 0.5
          collection.find(a: j)
        end
      end

      # thread that disconnects each server's pool
      threads << Thread.new do
        sleep 0.2

        client.cluster.servers_list.reverse.each do |server|
          if !server.arbiter?
            server.pool.disconnect!
          end
        end
      end
    end

    context 'all pools disconnected' do
      it_behaves_like 'does not raise error'
    end
  end

  describe 'when primary server is removed from topology' do
    let(:threads) do
      threads = []

      # thread that performs operations
      threads << Thread.new do
        10.times do |j|
          collection.find(a: j)
          sleep 0.5
          collection.find(a: j)
        end
      end

      # thread that marks removes the primary from the cluster
      threads << Thread.new do
        sleep 0.2
        server = client.cluster.next_primary
        client.cluster.remove(server.address.host)
      end
    end

    context 'when primary server is removed' do
      it_behaves_like 'does not raise error'
    end
  end

  describe 'when connection auth fails' do
    let(:options) do
      { max_pool_size: 5, min_pool_size: 5 }
    end

    let(:thread_count) { 10 }

    context 'when primary server is removed' do
      it 'works' do
        allow_any_instance_of(Mongo::Server::Connection).to receive(:connect!).and_wrap_original { |m, *args|
          if rand < 0.2
            raise Mongo::Error::SocketError
          else
            m.call(*args)
          end
        }

        threads

        expect {
          threads.collect { |t| t.join }
        }.not_to raise_error
      end
    end
  end
end

require 'spec_helper'

describe 'Connection pool stress test' do
  let(:options) do
    { max_pool_size: 5, min_pool_size: 3 }
  end

  let(:thread_count) { 5 }

  let(:documents) do
    [].tap do |documents|
      10000.times do |i|
        documents << { a: i}
      end
    end
  end

  let(:operation_threads) do
    [].tap do |threads|
      thread_count.times do |i|
        threads << Thread.new do
          10.times do |j|
            collection.find(a: i+j).to_a
            sleep 0.5
            collection.find(a: i+j).to_a
          end
        end
      end
    end
  end

  let(:client) do
    @client = authorized_client.with(options)
  end

  let!(:collection) do
    client[authorized_collection.name].tap do |collection|
      collection.drop
      collection.insert_many(documents)
    end
  end

  after do
    if @client
      @client.close(true)
    end
  end

  shared_examples_for 'does not raise error' do
    it 'does not raise error' do
      expect {
        threads.collect { |t| t.join }
      }.not_to raise_error
    end
  end

  describe 'when several threads run operations on the collection' do
    let(:threads) { operation_threads }

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

    context 'min pool size 5, max pool size 5' do
      let(:options) do
        { max_pool_size: 5, min_pool_size: 5 }
      end
      let(:thread_count) { 7 }

      it_behaves_like 'does not raise error'
    end
  end

  describe 'when there are many more threads than the max pool size' do
    let(:threads) { operation_threads }

    context '10 threads, max pool size 5' do
      let(:thread_count) { 10 }

      it_behaves_like 'does not raise error'
    end

    context '15 threads, max pool size 5' do
      let(:thread_count) { 15 }

      it_behaves_like 'does not raise error'
    end

    context '20 threads, max pool size 5' do
      let(:thread_count) { 20 }

      it_behaves_like 'does not raise error'
    end

    context '25 threads, max pool size 5' do
      let(:thread_count) { 20 }

      it_behaves_like 'does not raise error'
    end
  end

  context 'when primary pool is disconnected' do
    let(:threads) do
      threads = operation_threads

      # thread that disconnects primary's pool
      threads << Thread.new do
        sleep 0.2
        server = client.cluster.next_primary
        server.pool.disconnect!
      end
    end

    it_behaves_like 'does not raise error'
  end

  context 'when all pools are disconnected' do
    let(:threads) do
      threads = operation_threads

      # thread that disconnects each server's pool
      threads << Thread.new do
        sleep 0.2

        client.cluster.servers_list.reverse.each do |server|
          if server.description.data_bearing?
            server.pool.disconnect!
          end
        end
      end
    end

    it_behaves_like 'does not raise error'
  end

  # context 'when primary server is removed from topology' do
  #   require_topology :replica_set
  #   let(:threads) do
  #     threads = operation_threads

  #     # thread that removes the primary from the cluster
  #     threads << Thread.new do
  #       sleep 0.2
  #       server = client.cluster.next_primary
  #       client.cluster.remove(server.address.host)
  #       @removed_host = server.address.host
  #     end
  #   end

  #   it_behaves_like 'does not raise error'
  # end

  context 'when connection auth sometimes fails' do
    let(:threads) { operation_threads }

    # unlikely, but possible this fails. TODO: acceptable?
    it 'does not raise error', retry: 2 do
      allow_any_instance_of(Mongo::Server::Connection).to receive(:connect!).and_wrap_original { |m, *args|
        if rand < 0.01
          raise Mongo::Error::SocketError
        else
          m.call(*args)
        end
      }

      expect {
        threads.collect { |t| t.join }
      }.not_to raise_error
    end
  end
end

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
      10000.times do |i|
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

    it_behaves_like 'does not raise error'
  end

  context 'when all pools are disconnected' do
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

    it_behaves_like 'does not raise error'
  end

  context 'when primary server is removed from topology' do
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

    it_behaves_like 'does not raise error'
  end

  context 'when connection auth sometimes fails' do
    let(:options) do
      { max_pool_size: 5, min_pool_size: 5 }
    end
    let(:thread_count) { 10 }

    it 'does not raise error' do
      allow_any_instance_of(Mongo::Server::Connection).to receive(:connect!).and_wrap_original { |m, *args|
        if rand < 0.2
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

  describe 'timing' do
    let(:threads) do
      [].tap do |threads|
        thread_count.times do |i|
          threads << Thread.new do
            2000.times do |j|
              collection.find(a: i+j)
              sleep 0.001
              collection.find(a: i+j)
            end
          end
        end
      end
    end

    let(:thread_count) { 5 }

    context 'when there is no max idle time' do
      let(:options) do
        { max_pool_size: 10, min_pool_size: 5 }
      end

      it 'does not error' do
        start = Time.now
        expect {
          threads.collect { |t| t.join }
        }.not_to raise_error
        @duration = Time.now - start
        puts "No max idle time: #{@duration}"
      end
    end

    context 'when there is a low max idle time' do
      let(:options) do
        { max_pool_size: 10, min_pool_size: 5, max_idle_time: 0.0001 }
      end

      it 'does not error' do
        start = Time.now
        expect {
          threads.collect { |t| t.join }
        }.not_to raise_error
        @duration_with_idle_time = Time.now - start
        puts "Low max idle time: #{@duration_with_idle_time}"
      end
    end

    context 'when clear is called periodically' do
      let(:options) do
        { max_pool_size: 10, min_pool_size: 5 }
      end

      let(:threads) do
        threads = []
        thread_count.times do |i|
          threads << Thread.new do
            2000.times do |j|
              collection.find(a: i+j)
              sleep 0.001
              collection.find(a: i+j)
            end
          end
        end

        threads << Thread.new do
          10.times do
            client.cluster.next_primary.pool.clear
            sleep 0.01
          end
        end
        threads
      end

      it 'does not error' do
        start = Time.now
        expect {
          threads.collect { |t| t.join }
        }.not_to raise_error
        @duration_with_clear = Time.now - start
        puts "Clear called periodically: #{@duration_with_clear}"
      end
    end

    context 'when primary is changed' do
      min_server_fcv '4.2'
      require_topology :replica_set

      let(:options) do
        { max_pool_size: 10, min_pool_size: 5 }
      end

      let(:threads) do
        threads = []

        20.times do |i|
          threads << Thread.new do
            4000.times do |j|
              collection.find(a: i+j)
              sleep 0.001
              collection.find(a: i+j)
            end
          end
        end

        threads << Thread.new do
          sleep 1
          @exec_end = Time.now
          ClusterTools.instance.change_primary
          @start = Time.now
        end
        threads
      end

      it 'does not error' do
        threads
        @exec_start = Time.now
        expect {
          threads.collect { |t| t.join }
        }.not_to raise_error
        @after_primary_change_duration = Time.now - @start
        puts "Duration before primary change: #{@exec_end - @exec_start}"
        puts "Duration after primary change: #{@after_primary_change_duration}"
      end
    end
  end
end

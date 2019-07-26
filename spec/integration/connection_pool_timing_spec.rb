require 'spec_helper'

describe 'Connection pool timing test' do

  after(:all) do
    if ClusterConfig.instance.fcv_ish >= '4.2' && ClusterConfig.instance.topology == :replica_set
      ClusterTools.instance.set_election_timeout(10)
      ClusterTools.instance.set_election_handoff(true)
      ClusterTools.instance.reset_priorities
    end
  end

  # TODO: From step down spec; should update Cluster Tools
  # This setup reduces the runtime of the test and makes execution more
  # reliable. The spec as written requests a simple brute force step down,
  # but this causes intermittent failures.
  before(:all) do
    ClientRegistry.instance.close_all_clients
    Mongo::Monitoring::Global.subscribe(
      Mongo::Monitoring::CONNECTION_POOL,
      Mongo::Monitoring::CmapLogSubscriber.new)

    # These before/after blocks are run even if the tests themselves are
    # skipped due to server version not being appropriate
    if ClusterConfig.instance.fcv_ish >= '4.2' && ClusterConfig.instance.topology == :replica_set
      # It seems that a short election timeout can cause unintended elections,
      # which makes the server close connections which causes the driver to
      # reconnect which then fails the step down test.
      # The election timeout here is greater than the catch up period and
      # step down timeout specified in cluster tools.
      ClusterTools.instance.set_election_timeout(5)
      ClusterTools.instance.set_election_handoff(false)
    end
  end

  let(:client) do
    @client = authorized_client.with(options.merge(monitoring: true)).tap do |client|
      subscriber = Mongo::Monitoring::CmapLogSubscriber.new
      client.subscribe( Mongo::Monitoring::CONNECTION_POOL, subscriber )
    end
  end

  let!(:collection) do
    client[authorized_collection.name].tap do |collection|
      collection.drop
      collection.insert_many(documents)
    end
  end

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

    let(:more_threads) do
      [].tap do |more_threads|
        10.times do |i|
          threads << Thread.new do
            1.times do |j|
              collection.find(a: i+j).to_a
              sleep 0.001
              collection.find(a: i+j).to_a
            end
          end
        end
      end
    end

    let(:threads) do
      threads = []

      5.times do |i|
        threads << Thread.new do
          10.times do |j|
            collection.find(a: i+j).to_a
            sleep 0.001
            collection.find(a: i+j).to_a
          end
        end
      end

      threads << Thread.new do
        sleep 1
        @exec_end = Time.now
        ClusterTools.instance.change_primary
        @start = Time.now
        more_threads.collect { |t| t.join }
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

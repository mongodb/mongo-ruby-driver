require 'spec_helper'

describe 'Connection pool timing test' do
  require_stress
  clean_slate_for_all

  before(:all) do
    # This set up is taken from the step_down_spec file. In a future PR, ClusterTools
    # may be modified so this set up is no longer necessary.
    if ClusterConfig.instance.fcv_ish >= '4.2' && ClusterConfig.instance.topology == :replica_set
      ClusterTools.instance.set_election_timeout(5)
      ClusterTools.instance.set_election_handoff(false)
    end
  end

  after(:all) do
    if ClusterConfig.instance.fcv_ish >= '4.2' && ClusterConfig.instance.topology == :replica_set
      ClusterTools.instance.set_election_timeout(10)
      ClusterTools.instance.set_election_handoff(true)
      ClusterTools.instance.reset_priorities
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
          100.times do |j|
            collection.find(a: i+j).to_a
            sleep 0.01
            collection.find(a: i+j).to_a
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

    let(:threads) { operation_threads }

    it 'does not error' do
      start = Time.now
      expect {
        threads.collect { |t| t.join }
      }.not_to raise_error
      puts "[Connection Pool Timing] Duration with no max idle time: #{Time.now - start}"
    end
  end

  context 'when there is a low max idle time' do
    let(:options) do
      { max_pool_size: 10, min_pool_size: 5, max_idle_time: 0.1 }
    end

    let(:threads) { operation_threads }

    it 'does not error' do
      start = Time.now
      expect {
        threads.collect { |t| t.join }
      }.not_to raise_error
      puts "[Connection Pool Timing] Duration with low max idle time: #{Time.now - start}"
    end
  end

  context 'when clear is called periodically' do
    let(:options) do
      { max_pool_size: 10, min_pool_size: 5 }
    end

    let(:threads) do
      threads = operation_threads
      threads << Thread.new do
        10.times do
          sleep 0.1
          client.cluster.next_primary.pool.clear
        end
      end
      threads
    end

    it 'does not error' do
      start = Time.now
      expect {
        threads.collect { |t| t.join }
      }.not_to raise_error
      puts "[Connection Pool Timing] Duration when clear is called periodically: #{Time.now - start}"
    end
  end

  context 'when primary is changed, then more operations are performed' do
    min_server_fcv '4.2'
    require_topology :replica_set

    let(:options) do
      { max_pool_size: 10, min_pool_size: 5 }
    end

    let(:more_threads) do
      PossiblyConcurrentArray.new.tap do |more_threads|
        5.times do |i|
          more_threads << Thread.new do
            10.times do |j|
              collection.find(a: i+j).to_a
              sleep 0.01
              collection.find(a: i+j).to_a
            end
          end
        end
      end
    end

    let(:threads) do
      threads = PossiblyConcurrentArray.new

      5.times do |i|
        threads << Thread.new do
          10.times do |j|
            collection.find(a: i+j).to_a
            sleep 0.01
            collection.find(a: i+j).to_a
          end
        end
      end

      threads << Thread.new do
        # Wait for other threads to terminate first, otherwise we get an error
        # when trying to perform operations during primary change
        sleep 1

        @primary_change_start = Time.now
        ClusterTools.instance.change_primary
        @primary_change_end = Time.now

        # Primary change is complete; execute more operations
        more_threads.collect { |t| t.join }
      end
      threads
    end

    # On JRuby, sometimes the following error is produced indicating
    # possible data corruption or an interpreter bug:
    # RSpec::Expectations::ExpectationNotMetError: expected no Exception, got #<Mongo::Error::OperationFailure: Invalid ns [ruby-driver.] (73) (on localhost:27018, modern retry, attempt 1) (on localhost:27018, modern retry, attempt 1)>
    it 'does not error', retry: (BSON::Environment.jruby? ? 3 : 1) do
      threads
      start = Time.now
      expect do
        threads.each do |t|
          t.join
        end
      end.not_to raise_error
      puts "[Connection Pool Timing] Duration before primary change: #{@primary_change_start - start}. "\
        "Duration after primary change: #{Time.now - @primary_change_end}"
    end
  end
end

# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe 'Retryable reads errors tests' do

  let(:client) { authorized_client.with(options.merge(retry_reads: true)) }

  let(:collection) do
    client['retryable-reads-error-spec']
  end

  context "PoolClearedError retryability test" do
    require_topology :single, :replica_set, :sharded
    require_no_multi_mongos
    min_server_version '4.2.9'

    let(:options) { { max_pool_size: 1, heartbeat_frequency: 1000 } }

    let(:failpoint) do
      {
          configureFailPoint: "failCommand",
          mode: { times: 1 },
          data: {
              failCommands: [ "find" ],
              errorCode: 91,
              blockConnection: true,
              blockTimeMS: 1000
          }
      }
    end

    let(:subscriber) { Mrss::EventSubscriber.new }

    let(:threads) do
      threads = []
      threads << Thread.new do
        expect(collection.find(x: 1).first[:x]).to eq(1)
      end
      threads << Thread.new do
        expect(collection.find(x: 1).first[:x]).to eq(1)
      end
      threads
    end

    let(:find_events) do
      subscriber.started_events.select { |e| e.command_name == "find" }
    end

    let(:cmap_events) do
      subscriber.published_events
    end

    let(:event_types) do
      [
        Mongo::Monitoring::Event::Cmap::ConnectionCheckedOut,
        Mongo::Monitoring::Event::Cmap::ConnectionCheckOutFailed,
        Mongo::Monitoring::Event::Cmap::PoolCleared,
      ]
    end

    let(:check_out_results) do
      cmap_events.select do |e|
        event_types.include?(e.class)
      end
    end

    before do
      collection.insert_one(x: 1)
      authorized_client.use(:admin).command(failpoint)
      client.subscribe(Mongo::Monitoring::COMMAND, subscriber)
      client.subscribe(Mongo::Monitoring::CONNECTION_POOL, subscriber)
    end

    it "retries on PoolClearedError" do
      # After the first find fails, the pool is paused and retry is triggered.
      # Now, a race is started between the second find acquiring a connection,
      # and the first retrying the read. Now, retry reads cause the cluster to
      # be rescanned and the pool to be unpaused, allowing the second checkout
      # to succeed (when it should fail). Therefore we want the second find's
      # check out to win the race. This gives the check out a little head start.
      allow_any_instance_of(Mongo::Server::ConnectionPool).to receive(:ready).and_wrap_original do |m, *args, &block|
        ::Utils.wait_for_condition(5) do
          # check_out_results should contain:
          # - find1 connection check out successful
          # - pool cleared
          # - find2 connection check out failed
          # We wait here for the third event to happen before we ready the pool.
          cmap_events.select do |e|
            event_types.include?(e.class)
          end.length >= 3
        end
        m.call(*args, &block)
      end
      threads.map(&:join)
      expect(check_out_results[0]).to be_a(Mongo::Monitoring::Event::Cmap::ConnectionCheckedOut)
      expect(check_out_results[1]).to be_a(Mongo::Monitoring::Event::Cmap::PoolCleared)
      expect(check_out_results[2]).to be_a(Mongo::Monitoring::Event::Cmap::ConnectionCheckOutFailed)
      expect(find_events.length).to eq(3)
    end

    after do
      authorized_client.use(:admin).command({
        configureFailPoint: "failCommand",
        mode: "off",
      })
    end
  end

  context 'retry on different mongos' do
    require_topology :sharded

    let(:first_mongos) do
      Mongo::Client.new(
        [SpecConfig.instance.addresses.first],
        direct_connection: true,
        database: 'admin'
      )
    end

    let(:second_mongos) do
      Mongo::Client.new(
        [SpecConfig.instance.addresses.last],
        direct_connection: false,
        database: 'admin'
      )
    end

    let(:client) { authorized_client.with(retry_reads: true) }

    before do
      skip 'This test requires two mongos' unless SpecConfig.instance.addresses.length == 2

      first_mongos.database.command(
        configureFailPoint: 'failCommand',
          mode: { times: 1 },
          data: {
            failCommands: %w(find),
            closeConnection: false,
            errorCode: 11600,
            errorLabels: ['RetryableWriteError']
          },
      )

      second_mongos.database.command(
        configureFailPoint: 'failCommand',
          mode: { times: 1 },
          data: {
            failCommands: %w(find),
            closeConnection: false,
            errorCode: 11600,
            errorLabels: ['RetryableWriteError']
          },
      )
    end

    let(:subscriber) { Mrss::EventSubscriber.new }

    let(:find_started_events) do
      subscriber.started_events.select { |e| e.command_name == "find" }
    end

    let(:find_failed_events) do
      subscriber.failed_events.select { |e| e.command_name == "find" }
    end

    after do
      first_mongos.close
      second_mongos.close
    end

    it 'retries on different mongos' do
      client.subscribe(Mongo::Monitoring::COMMAND, subscriber)
      expect { collection.find.first }.to raise_error
      expect(find_started_events.map { |e| e.address.to_s }.sort).to eq(SpecConfig.instance.addresses.map { |a| a.to_s }.sort)
      expect(find_failed_events.map { |e| e.address.to_s }.sort).to eq(SpecConfig.instance.addresses.map { |a| a.to_s }.sort)
    end
  end
end

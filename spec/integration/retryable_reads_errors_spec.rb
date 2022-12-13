# frozen_string_literal: true
# encoding: utf-8

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
        Thread.current['mongo:thread']='thread1'
        expect(collection.find(x: 1).first[:x]).to eq(1)
      end
      threads << Thread.new do
        Thread.current['mongo:thread']='thread2'
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
      client.log_info("\n--------------- START ---------------\n\n")

      # After the first find fails, the pool is paused and retry is triggered.
      # Now, a race is started between the second find acquiring a connection,
      # and the first retrying the read. Now, retry reads cause the cluster to
      # be rescanned and the pool to be unpaused, allowing the second checkout
      # to succeed (when it should fail). Therefore we want the second find's
      # check out to win the race. This gives the check out a little head start.
      allow(client.cluster).to receive(:scan!).and_wrap_original do |m, *args, &block|
        client.log_info("SCANNN #{Thread.current['mongo:thread']}")
        ::Utils.wait_for_condition(5) do
          # check_out_results should contain:
          # - find1 connection check out successful
          # - pool cleared
          # - find2 connection check out failed
          # We wait here for the third event to happen before we scan and rediscover the server.
          cmap_events.select do |e|
            event_types.include?(e.class)
          end.length >= 3
        end
        client.log_info("DONE WAITING SCANNN #{Thread.current['mongo:thread']}")
        m.call(*args, &block)
      end
      Mongo.broken_view_options = false
      threads.map(&:join)
      Mongo.broken_view_options = true
      client.log_info("CHECKOUT RESULTS #{check_out_results.map(&:class)}")
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
end

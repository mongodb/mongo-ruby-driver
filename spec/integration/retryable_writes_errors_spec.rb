# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe 'Retryable writes errors tests' do

  let(:options) { {} }

  let(:client) do
    authorized_client.with(options.merge(retry_writes: true))
  end

  let(:collection) do
    client['retryable-writes-error-spec']
  end

  context 'when the storage engine does not support retryable writes but the server does' do
    require_mmapv1
    min_server_fcv '3.6'
    require_topology :replica_set, :sharded

    before do
      collection.delete_many
    end

    context 'when a retryable write is attempted' do
      it 'raises an actionable error message' do
        expect {
          collection.insert_one(a:1)
        }.to raise_error(Mongo::Error::OperationFailure, /This MongoDB deployment does not support retryable writes. Please add retryWrites=false to your connection string or use the retry_writes: false Ruby client option/)
        expect(collection.find.count).to eq(0)
      end
    end
  end

  context "when encountering a NoWritesPerformed error after an error with a RetryableWriteError label" do
    require_topology :replica_set
    require_retry_writes
    min_server_version '4.4'

    let(:failpoint1) do
      {
        configureFailPoint: "failCommand",
        mode: { times: 1 },
        data: {
          writeConcernError: {
            code: 91,
            errorLabels: ["RetryableWriteError"],
          },
          failCommands: ["insert"],
        }
      }
    end

    let(:failpoint2) do
      {
        configureFailPoint: "failCommand",
        mode: { times: 1 },
        data: {
          errorCode: 10107,
          errorLabels: ["RetryableWriteError", "NoWritesPerformed"],
          failCommands: ["insert"],
        },
      }
    end

    let(:subscriber) { Mrss::EventSubscriber.new }

    before do
      authorized_client.subscribe(Mongo::Monitoring::COMMAND, subscriber)
      authorized_client.use(:admin).command(failpoint1)

      expect(authorized_collection.write_worker).to receive(:retry_write).once.and_wrap_original do |m, *args, **kwargs, &block|
        expect(args.first.code).to eq(91)
        authorized_client.use(:admin).command(failpoint2)
        m.call(*args, **kwargs, &block)
      end
    end

    after do
      authorized_client.use(:admin).command({
        configureFailPoint: "failCommand",
        mode: "off",
      })
    end

    it "returns the original error" do
      expect do
        authorized_collection.insert_one(x: 1)
      end.to raise_error(Mongo::Error::OperationFailure, /\[91\]/)
    end
  end

  context "PoolClearedError retryability test" do
    require_topology :single, :replica_set, :sharded
    require_no_multi_mongos
    require_fail_command
    require_retry_writes

    let(:options) { { max_pool_size: 1 } }

    let(:failpoint) do
      {
          configureFailPoint: "failCommand",
          mode: { times: 1 },
          data: {
              failCommands: [ "insert" ],
              errorCode: 91,
              blockConnection: true,
              blockTimeMS: 1000,
              errorLabels: ["RetryableWriteError"]
          }
      }
    end

    let(:subscriber) { Mrss::EventSubscriber.new }

    let(:threads) do
      threads = []
      threads << Thread.new do
        expect(collection.insert_one(x: 2)).to be_successful
      end
      threads << Thread.new do
        expect(collection.insert_one(x: 2)).to be_successful
      end
      threads
    end

    let(:insert_events) do
      subscriber.started_events.select { |e| e.command_name == "insert" }
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
      authorized_client.use(:admin).command(failpoint)
      client.subscribe(Mongo::Monitoring::COMMAND, subscriber)
      client.subscribe(Mongo::Monitoring::CONNECTION_POOL, subscriber)
    end

    it "retries on PoolClearedError" do
      # After the first insert fails, the pool is paused and retry is triggered.
      # Now, a race is started between the second insert acquiring a connection,
      # and the first retrying the read. Now, retry reads cause the cluster to
      # be rescanned and the pool to be unpaused, allowing the second checkout
      # to succeed (when it should fail). Therefore we want the second insert's
      # check out to win the race. This gives the check out a little head start.
      allow_any_instance_of(Mongo::Server::ConnectionPool).to receive(:ready).and_wrap_original do |m, *args, &block|
        ::Utils.wait_for_condition(5) do
          # check_out_results should contain:
          # - insert1 connection check out successful
          # - pool cleared
          # - insert2 connection check out failed
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
      expect(insert_events.length).to eq(3)
    end

    after do
      authorized_client.use(:admin).command({
        configureFailPoint: "failCommand",
        mode: "off",
      })
    end
  end
end

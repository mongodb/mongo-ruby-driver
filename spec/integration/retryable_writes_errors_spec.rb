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

  context 'Retries in a sharded cluster' do
    require_topology :sharded
    min_server_version '4.2'
    require_no_auth

    let(:subscriber) { Mrss::EventSubscriber.new }

    let(:insert_started_events) do
      subscriber.started_events.select { |e| e.command_name == "insert" }
    end

    let(:insert_failed_events) do
      subscriber.failed_events.select { |e| e.command_name == "insert" }
    end

    let(:insert_succeeded_events) do
      subscriber.succeeded_events.select { |e| e.command_name == "insert" }
    end

    context 'when another mongos is available' do

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

      let(:client) do
        new_local_client(
          [
            SpecConfig.instance.addresses.first,
            SpecConfig.instance.addresses.last,
          ],
          SpecConfig.instance.test_options.merge(retry_writes: true)
        )
      end

      let(:expected_servers) do
        [
          SpecConfig.instance.addresses.first.to_s,
          SpecConfig.instance.addresses.last.to_s
        ].sort
      end

      before do
        skip 'This test requires at least two mongos' if SpecConfig.instance.addresses.length < 2

        first_mongos.database.command(
          configureFailPoint: 'failCommand',
          mode: { times: 1 },
          data: {
            failCommands: %w(insert),
            closeConnection: false,
            errorCode: 6,
            errorLabels: ['RetryableWriteError']
          }
        )

        second_mongos.database.command(
          configureFailPoint: 'failCommand',
          mode: { times: 1 },
          data: {
            failCommands: %w(insert),
            closeConnection: false,
            errorCode: 6,
            errorLabels: ['RetryableWriteError']
          }
        )
      end

      after do
        [first_mongos, second_mongos].each do |admin_client|
          admin_client.database.command(
            configureFailPoint: 'failCommand',
            mode: 'off'
          )
          admin_client.close
        end
        client.close
      end

      it 'retries on different mongos' do
        client.subscribe(Mongo::Monitoring::COMMAND, subscriber)
        expect { collection.insert_one(x: 1) }.to raise_error(Mongo::Error::OperationFailure)
        expect(insert_started_events.map { |e| e.address.to_s }.sort).to eq(expected_servers)
        expect(insert_failed_events.map { |e| e.address.to_s }.sort).to eq(expected_servers)
      end
    end

    context 'when no other mongos is available' do
      let(:mongos) do
        Mongo::Client.new(
          [SpecConfig.instance.addresses.first],
          direct_connection: true,
          database: 'admin'
        )
      end

      let(:client) do
        new_local_client(
          [
            SpecConfig.instance.addresses.first
          ],
          SpecConfig.instance.test_options.merge(retry_writes: true)
        )
      end

      before do
        mongos.database.command(
          configureFailPoint: 'failCommand',
          mode: { times: 1 },
          data: {
            failCommands: %w(insert),
            closeConnection: false,
            errorCode: 6,
            errorLabels: ['RetryableWriteError']
          }
        )
      end

      after do
        mongos.database.command(
          configureFailPoint: 'failCommand',
          mode: 'off'
        )
        mongos.close
        client.close
      end

      it 'retries on the same mongos' do
        client.subscribe(Mongo::Monitoring::COMMAND, subscriber)
        expect { collection.insert_one(x: 1) }.not_to raise_error
        expect(insert_started_events.map { |e| e.address.to_s }.sort).to eq([
          SpecConfig.instance.addresses.first.to_s,
          SpecConfig.instance.addresses.first.to_s
        ])
        expect(insert_failed_events.map { |e| e.address.to_s }.sort).to eq([
          SpecConfig.instance.addresses.first.to_s
        ])
        expect(insert_succeeded_events.map { |e| e.address.to_s }.sort).to eq([
          SpecConfig.instance.addresses.first.to_s
        ])
      end
    end
  end
end

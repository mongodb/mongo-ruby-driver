# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe 'Retryable reads errors tests' do
  retry_test

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

    shared_examples_for 'retries on PoolClearedError' do
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
    end

    it_behaves_like 'retries on PoolClearedError'

    context 'legacy read retries' do

      let(:client) { authorized_client.with(options.merge(retry_reads: false, max_read_retries: 1)) }

      it_behaves_like 'retries on PoolClearedError'
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

    let(:find_started_events) do
      subscriber.started_events.select { |e| e.command_name == "find" }
    end

    let(:find_failed_events) do
      subscriber.failed_events.select { |e| e.command_name == "find" }
    end

    let(:find_succeeded_events) do
      subscriber.succeeded_events.select { |e| e.command_name == "find" }
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
          SpecConfig.instance.test_options.merge(retry_reads: true)
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
            failCommands: %w(find),
            closeConnection: false,
            errorCode: 6
          }
        )

        second_mongos.database.command(
          configureFailPoint: 'failCommand',
          mode: { times: 1 },
          data: {
            failCommands: %w(find),
            closeConnection: false,
            errorCode: 6
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
        expect { collection.find.first }.to raise_error(Mongo::Error::OperationFailure)
        expect(find_started_events.map { |e| e.address.to_s }.sort).to eq(expected_servers)
        expect(find_failed_events.map { |e| e.address.to_s }.sort).to eq(expected_servers)
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
          SpecConfig.instance.test_options.merge(retry_reads: true)
        )
      end

      before do
        mongos.database.command(
          configureFailPoint: 'failCommand',
          mode: { times: 1 },
          data: {
            failCommands: %w(find),
            closeConnection: false,
            errorCode: 6
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
        expect { collection.find.first }.not_to raise_error
        expect(find_started_events.map { |e| e.address.to_s }.sort).to eq([
          SpecConfig.instance.addresses.first.to_s,
          SpecConfig.instance.addresses.first.to_s
        ])
        expect(find_failed_events.map { |e| e.address.to_s }.sort).to eq([
          SpecConfig.instance.addresses.first.to_s
        ])
        expect(find_succeeded_events.map { |e| e.address.to_s }.sort).to eq([
          SpecConfig.instance.addresses.first.to_s
        ])
      end
    end
  end
end

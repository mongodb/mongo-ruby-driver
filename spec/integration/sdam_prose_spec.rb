# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe 'SDAM prose tests' do
  # The "streaming protocol tests" are covered by the tests in
  # sdam_events_spec.rb.

  describe 'RTT tests' do
    min_server_fcv '4.4'
    require_topology :single

    let(:subscriber) { Mrss::EventSubscriber.new }

    let(:client) do
      new_local_client(SpecConfig.instance.addresses,
        # Heartbeat interval is bound by 500 ms
        SpecConfig.instance.test_options.merge(
          heartbeat_frequency: 0.5,
          app_name: 'streamingRttTest',
        ),
      ).tap do |client|
        client.subscribe(Mongo::Monitoring::SERVER_HEARTBEAT, subscriber)
      end
    end

    it 'updates RTT' do
      server = client.cluster.next_primary

      sleep 2

      events = subscriber.select_succeeded_events(Mongo::Monitoring::Event::ServerHeartbeatSucceeded)
      events.each do |event|
        event.round_trip_time.should be_a(Numeric)
        event.round_trip_time.should > 0
      end

      root_authorized_client.use('admin').database.command(
        configureFailPoint: 'failCommand',
        mode: {times: 1000},
        data: {
          failCommands: %w(isMaster hello),
          blockConnection: true,
          blockTimeMS: 500,
          appName: "streamingRttTest",
        },
      )

      deadline = Mongo::Utils.monotonic_time + 10
      loop do
        if server.average_round_trip_time > 0.25
          break
        end
        if Mongo::Utils.monotonic_time >= deadline
          raise "Failed to witness RTT growing to >= 250 ms in 10 seconds"
        end
        sleep 0.2
      end
    end

    after do
      root_authorized_client.use('admin').database.command(
        configureFailPoint: 'failCommand', mode: 'off')
    end
  end

  describe 'Connection Pool Backpressure' do
    min_server_fcv '8.2'
    require_topology :single

    let(:subscriber) { Mrss::EventSubscriber.new }

    let(:client) do
      new_local_client(
        SpecConfig.instance.addresses,
        SpecConfig.instance.all_test_options.merge(
          max_connecting: 100,
          max_pool_size: 100,
        ),
      ).tap do |client|
        client.subscribe(Mongo::Monitoring::CONNECTION_POOL, subscriber)
      end
    end

    after do
      sleep 1
      admin_db = root_authorized_client.use('admin').database

      if defined?(@prev_ingressConnectionEstablishmentRateLimiterEnabled) &&
         defined?(@prev_ingressConnectionEstablishmentRatePerSec) &&
         defined?(@prev_ingressConnectionEstablishmentBurstCapacitySecs) &&
         defined?(@prev_ingressConnectionEstablishmentMaxQueueDepth)
        admin_db.command(
          setParameter: 1,
          ingressConnectionEstablishmentRateLimiterEnabled: @prev_ingressConnectionEstablishmentRateLimiterEnabled,
        )
        admin_db.command(
          setParameter: 1,
          ingressConnectionEstablishmentRatePerSec: @prev_ingressConnectionEstablishmentRatePerSec,
        )
        admin_db.command(
          setParameter: 1,
          ingressConnectionEstablishmentBurstCapacitySecs: @prev_ingressConnectionEstablishmentBurstCapacitySecs,
        )
        admin_db.command(
          setParameter: 1,
          ingressConnectionEstablishmentMaxQueueDepth: @prev_ingressConnectionEstablishmentMaxQueueDepth,
        )
      else
        # Fallback: at least disable the limiter if previous values were not captured.
        admin_db.command(
          setParameter: 1,
          ingressConnectionEstablishmentRateLimiterEnabled: false,
        )
      end
    end

    it 'generates checkout failures when the ingress connection rate limiter is active' do
      admin_db = root_authorized_client.use('admin').database

      # Capture current ingress connection establishment parameters so they can be restored.
      current_params = admin_db.command(
        getParameter: 1,
        ingressConnectionEstablishmentRateLimiterEnabled: 1,
        ingressConnectionEstablishmentRatePerSec: 1,
        ingressConnectionEstablishmentBurstCapacitySecs: 1,
        ingressConnectionEstablishmentMaxQueueDepth: 1,
      ).first

      @prev_ingressConnectionEstablishmentRateLimiterEnabled =
        current_params['ingressConnectionEstablishmentRateLimiterEnabled']
      @prev_ingressConnectionEstablishmentRatePerSec =
        current_params['ingressConnectionEstablishmentRatePerSec']
      @prev_ingressConnectionEstablishmentBurstCapacitySecs =
        current_params['ingressConnectionEstablishmentBurstCapacitySecs']
      @prev_ingressConnectionEstablishmentMaxQueueDepth =
        current_params['ingressConnectionEstablishmentMaxQueueDepth']

      # Enable the ingress rate limiter with test-specific values.
      admin_db.command(
        setParameter: 1,
        ingressConnectionEstablishmentRateLimiterEnabled: true,
      )
      admin_db.command(
        setParameter: 1,
        ingressConnectionEstablishmentRatePerSec: 20,
      )
      admin_db.command(
        setParameter: 1,
        ingressConnectionEstablishmentBurstCapacitySecs: 1,
      )
      admin_db.command(
        setParameter: 1,
        ingressConnectionEstablishmentMaxQueueDepth: 1,
      )

      # Add a document so $where has something to process.
      client.use('test')['test'].delete_many
      client.use('test')['test'].insert_one({})

      # Run 100 parallel find_one operations that contend for connections.
      threads = 100.times.map do
        Thread.new do
          begin
            client.use('test')['test'].find(
              '$where' => 'function() { sleep(2000); return true; }'
            ).first
          rescue Mongo::Error::PoolTimeout,
                 Mongo::Error::SocketError,
                 Mongo::Error::NoServerAvailable
            # Ignore connection errors (including checkout timeouts).
          end
        end
      end
      threads.each(&:join)

      checkout_failed = subscriber.select_published_events(
        Mongo::Monitoring::Event::Cmap::ConnectionCheckOutFailed
      )

      expect(checkout_failed.length).to be >= 10
    end
  end
end

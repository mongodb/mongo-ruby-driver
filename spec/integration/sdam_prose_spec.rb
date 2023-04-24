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
end

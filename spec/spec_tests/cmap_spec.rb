# frozen_string_literal: true
# encoding: utf-8

require 'spec_helper'

require 'runners/cmap'

# Temporary scopes in all of the tests are needed to exclude endSessions
# commands being sent during cleanup from interfering with assertions.

describe 'Cmap' do
  clean_slate

  declare_topology_double

  let(:cluster) do
    double('cluster').tap do |cl|
      allow(cl).to receive(:topology).and_return(topology)
      allow(cl).to receive(:options).and_return({})
      allow(cl).to receive(:app_metadata).and_return(Mongo::Server::AppMetadata.new({}))
      allow(cl).to receive(:run_sdam_flow)
      allow(cl).to receive(:update_cluster_time)
      allow(cl).to receive(:cluster_time).and_return(nil)
    end
  end

  let(:options) do
    Mongo::Utils.shallow_symbolize_keys(Mongo::Client.canonicalize_ruby_options(
      SpecConfig.instance.all_test_options,
    )).update(monitoring_io: false, populator_io: true).tap do |options|
      # We have a wait queue timeout set in the test suite options, but having
      # this option set interferes with assertions in the cmap spec tests.
      options.delete(:wait_queue_timeout)
    end
  end

  CMAP_TESTS.each do |file|
    spec = Mongo::Cmap::Spec.new(file)

    context("#{spec.description} (#{file.sub(%r'.*/data/cmap/', '')})") do
      unless spec.satisfied?
        before(:all) do
          skip "Requirements not satisfied"
        end
      end

      before do
        subscriber = Mrss::EventSubscriber.new

        monitoring = Mongo::Monitoring.new(monitoring: false)
        monitoring.subscribe(Mongo::Monitoring::CONNECTION_POOL, subscriber)

        @server = register_server(
          Mongo::Server.new(
            ClusterConfig.instance.primary_address,
            cluster,
            monitoring,
            Mongo::Event::Listeners.new,
            options.merge(spec.pool_options)
          ).tap do |server|
            allow(server).to receive(:description).and_return(ClusterConfig.instance.primary_description)
          end
        )

        @client = ClusterTools.instance.direct_client(ClusterConfig.instance.primary_address,
          database: 'admin')
        spec.setup(@server, @client, subscriber)
      end

      after do
        if pool = @server&.pool_internal
          pool.disconnect!
        end

        spec.pool&.close
      end

      let!(:result) do
        if @server.load_balancer?
          allow_any_instance_of(Mongo::Server::Connection).to receive(:service_id).and_return('very fake')
        end
        spec.run
      end

      let(:verifier) do
        Mongo::Cmap::Verifier.new(spec)
      end

      it 'raises the correct error' do
        RSpec::Mocks.with_temporary_scope do
          expect(result['error']).to eq(spec.expected_error)
        end
      end

      let(:actual_events) { result['events'].freeze }

      it 'emits the correct number of events' do
        RSpec::Mocks.with_temporary_scope do
          expect(actual_events.length).to eq(spec.expected_events.length)
        end
      end

      spec.expected_events.each_with_index do |expected_event, index|
        it "emits correct event #{index+1}" do
          RSpec::Mocks.with_temporary_scope do
            verifier.verify_hashes(actual_events[index], expected_event)
          end
        end
      end
    end
  end
end

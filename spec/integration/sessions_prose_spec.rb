# frozen_string_literal: true

require 'spec_helper'

describe 'sessions prose tests', type: :feature do
  context 'when setting both ``snapshot`` and ``causalConsistency`` to true' do
    min_server_version '5.0'
    require_topology :replica_set, :sharded

    it 'is not allowed' do
      expect do
        authorized_client.start_session(snapshot: true, causal_consistency: true)
      end.to raise_error ArgumentError, /(snapshot.*causal_consistency)|(causal_consistency.*snapshot)/
    end
  end

  context 'if pool is LIFO' do
    it 'returns session IDs in LIFO order' do
      a = authorized_client.start_session
      b = authorized_client.start_session
      
      a_id = a.session_id
      b_id = b.session_id

      a.end_session
      b.end_session

      c = authorized_client.start_session
      d = authorized_client.start_session

      expect(b_id).to eq c.session_id
      expect(a_id).to eq d.session_id
    end
  end

  context '``$clusterTime'' in commands' do
    let(:client) do
      authorized_client.with(database: "test", heartbeat_frequency: 1_000_000)
    end

    let(:cluster_time_available) do
      client.cluster.servers.first.max_wire_version >= 6
    end

    let(:subscriber) do
      assertor = ->(condition) { expect(condition).to be true }
      SessionClusterTimeTestSubscriber.new(command, assertor)
    end

    let(:subscribed_client) do
      client.tap do |c|
        c.subscribe(Mongo::Monitoring::COMMAND, subscriber)
      end
    end

    before { skip "$clusterTime is not available" unless cluster_time_available }

    shared_examples '$clusterTime' do
      it "is reported successfully" do
        expect(subscriber.finished?).to be false
        run_command # first time, to store $clusterTime
        run_command # second time, to compare $clusterTime to the stored value
        expect(subscriber.finished?).to be true
      end
    end

    context 'ping' do
      let(:command) { "ping" }

      def run_command
        subscribed_client.database.command ping: 1
      end

      include_examples '$clusterTime'
    end

    context 'aggregate' do
      let(:command) { "aggregate" }

      def run_command
        subscribed_client.database
          .aggregate([ { "$listLocalSessions" => {} } ]).to_a
      end

      include_examples '$clusterTime'
    end

    context 'find' do
      let(:command) { "find" }

      def run_command
        subscribed_client["coll"].find.to_a
      end

      include_examples '$clusterTime'
    end

    context 'insert' do
      let(:command) { "insert" }

      def run_command
        subscribed_client["coll"].insert_one name: "test"
      end

      include_examples '$clusterTime'
    end
  end
end

class SessionClusterTimeTestSubscriber
  attr_reader :command_name, :cluster_time

  def initialize(command_name, assertor)
    @command_name = command_name
    @assertor = assertor
    @finished = false
    @cluster_time = nil
  end

  def started(event)
    if event.command_name == command_name
      if cluster_time
        @finished = true
        assert event.command["$clusterTime"] == cluster_time
      else
        assert event.command.key?("$clusterTime")
      end
    end
  end

  def succeeded(event)
    if event.command_name == command_name
      @cluster_time = event.reply["$clusterTime"]
    end
  end

  def finished?
    @finished
  end

  private

    def assert(condition)
      @assertor[condition]
    end
end

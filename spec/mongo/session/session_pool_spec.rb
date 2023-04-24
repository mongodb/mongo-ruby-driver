# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe Mongo::Session::SessionPool do
  min_server_fcv '3.6'
  require_topology :replica_set, :sharded, :load_balanced
  clean_slate_for_all

  let(:cluster) do
    authorized_client.cluster.tap do |cluster|
      # Cluster time assertions can fail if there are background operations
      # that cause cluster time to be updated. This also necessitates clean
      # state requirement.
      authorized_client.close
    end
  end

  describe '.create' do

    let!(:pool) do
      described_class.create(cluster)
    end

    it 'creates a session pool' do
      expect(pool).to be_a(Mongo::Session::SessionPool)
    end

    it 'adds the pool as an instance variable on the cluster' do
      expect(cluster.session_pool).to eq(pool)
    end
  end

  describe '#initialize' do

    let(:pool) do
      described_class.new(cluster)
    end

    it 'sets the cluster' do
      expect(pool.instance_variable_get(:@cluster)).to be(authorized_client.cluster)
    end
  end

  describe '#inspect' do

    let(:pool) do
      described_class.new(cluster)
    end

    before do
      s = pool.checkout
      pool.checkin(s)
    end

    it 'includes the Ruby object_id in the formatted string' do
      expect(pool.inspect).to include(pool.object_id.to_s)
    end

    it 'includes the pool size in the formatted string' do
      expect(pool.inspect).to include('current_size=1')
    end
  end

  describe 'checkout' do

    let(:pool) do
      described_class.new(cluster)
    end

    context 'when a session is checked out' do

      let!(:session_a) do
        pool.checkout
      end

      let!(:session_b) do
        pool.checkout
      end

      before do
        pool.checkin(session_a)
        pool.checkin(session_b)
      end

      it 'is returned to the front of the queue' do
        expect(pool.checkout).to be(session_b)
        expect(pool.checkout).to be(session_a)
      end
    end

    context 'when there are sessions about to expire in the queue' do

      let(:old_session_a) do
        pool.checkout
      end

      let(:old_session_b) do
        pool.checkout
      end

      before do
        pool.checkin(old_session_a)
        pool.checkin(old_session_b)
        allow(old_session_a).to receive(:last_use).and_return(Time.now - 1800)
        allow(old_session_b).to receive(:last_use).and_return(Time.now - 1800)
      end

      context 'when a session is checked out' do

        let(:checked_out_session) do
          pool.checkout
        end

        context "in non load-balanced topology" do
          require_topology :replica_set, :sharded

          it 'disposes of the old session and returns a new one' do
            old_sessions = [old_session_a, old_session_b]
            expect(old_sessions).not_to include(pool.checkout)
            expect(old_sessions).not_to include(pool.checkout)
            expect(pool.instance_variable_get(:@queue)).to be_empty
          end
        end

        context "in load-balanced topology" do
          require_topology :load_balanced

          it 'doed not dispose of the old session' do
            old_sessions = [old_session_a, old_session_b]
            expect(old_sessions).to include(checked_out_session)
            expect(old_sessions).to include(checked_out_session)
            expect(pool.instance_variable_get(:@queue)).to be_empty
          end
        end
      end
    end

    context 'when a sessions that is about to expire is checked in' do

      let(:old_session_a) do
        pool.checkout
      end

      let(:old_session_b) do
        pool.checkout
      end

      before do
        allow(old_session_a).to receive(:last_use).and_return(Time.now - 1800)
        allow(old_session_b).to receive(:last_use).and_return(Time.now - 1800)
        pool.checkin(old_session_a)
        pool.checkin(old_session_b)
      end

      context "in non load-balanced topology" do
        require_topology :replica_set, :sharded

        it 'disposes of the old sessions instead of adding them to the pool' do
          old_sessions = [old_session_a, old_session_b]
          expect(old_sessions).not_to include(pool.checkout)
          expect(old_sessions).not_to include(pool.checkout)
          expect(pool.instance_variable_get(:@queue)).to be_empty
        end
      end

      context "in load-balanced topology" do
        require_topology :load_balanced

        it 'does not dispose of the old sessions' do
          old_sessions = [old_session_a, old_session_b]
          expect(old_sessions).to include(pool.checkout)
          expect(old_sessions).to include(pool.checkout)
          expect(pool.instance_variable_get(:@queue)).to be_empty
        end
      end
    end
  end

  describe '#end_sessions' do

    let(:pool) do
      described_class.create(client.cluster)
    end

    let!(:session_a) do
      pool.checkout
    end

    let!(:session_b) do
      pool.checkout
    end

    let(:subscriber) { Mrss::EventSubscriber.new }

    let(:client) do
      authorized_client.tap do |client|
        client.subscribe(Mongo::Monitoring::COMMAND, subscriber)
      end
    end

    context 'when the number of ids is not larger than 10,000' do

      before do
        client.database.command(ping: 1)
        pool.checkin(session_a)
        pool.checkin(session_b)
      end

      let!(:cluster_time) do
        client.cluster.cluster_time
      end

      let(:end_sessions_command) do
        pool.end_sessions
        subscriber.started_events.find { |c| c.command_name == 'endSessions'}
      end

      it 'sends the endSessions command with all the session ids' do
        end_sessions_command
        expect(end_sessions_command.command[:endSessions]).to include(BSON::Document.new(session_a.session_id))
        expect(end_sessions_command.command[:endSessions]).to include(BSON::Document.new(session_b.session_id))
      end

      context 'when talking to a replica set or mongos' do

        it 'sends the endSessions command with all the session ids and cluster time' do
          start_time = client.cluster.cluster_time
          end_sessions_command
          end_time = client.cluster.cluster_time
          expect(end_sessions_command.command[:endSessions]).to include(BSON::Document.new(session_a.session_id))
          expect(end_sessions_command.command[:endSessions]).to include(BSON::Document.new(session_b.session_id))
          # cluster time may have been advanced due to background operations
          actual_cluster_time = Mongo::ClusterTime.new(end_sessions_command.command[:$clusterTime])
          expect(actual_cluster_time).to be >= start_time
          expect(actual_cluster_time).to be <= end_time
        end
      end
    end

    context 'when the number of ids is larger than 10_000' do

      let(:ids) do
        10_001.times.map do |i|
          bytes = [SecureRandom.uuid.gsub(/\-/, '')].pack('H*')
          BSON::Document.new(id: BSON::Binary.new(bytes, :uuid))
        end
      end

      before do
        queue = []
        ids.each do |id|
          queue << double('session', session_id: id)
        end
        pool.instance_variable_set(:@queue, queue)
        expect(Mongo::Operation::Command).to receive(:new).at_least(:twice).and_call_original
      end

      let(:end_sessions_commands) do
        subscriber.started_events.select { |c| c.command_name == 'endSessions'}
      end

      it 'sends the command more than once' do
        pool.end_sessions
        expect(end_sessions_commands.size).to eq(2)
        expect(end_sessions_commands[0].command[:endSessions]).to eq(ids[0...10_000])
        expect(end_sessions_commands[1].command[:endSessions]).to eq([ids[10_000]])
      end
    end
  end
end

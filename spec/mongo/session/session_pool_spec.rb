require 'spec_helper'

describe Mongo::Session::SessionPool do

  describe '.create' do

    let(:client) do
      authorized_client
    end

    let!(:pool) do
      described_class.create(client)
    end

    it 'creates a session pool' do
      expect(pool).to be_a(Mongo::Session::SessionPool)
    end

    it 'adds the pool as an instance variable on the client' do
      expect(client.instance_variable_get(:@session_pool)).to eq(pool)
    end
  end

  describe '#initialize' do

    let(:pool) do
      described_class.new(authorized_client)
    end

    it 'sets the client' do
      expect(pool.instance_variable_get(:@client)).to be(authorized_client)
    end
  end

  describe 'checkout', if: sessions_enabled? do

    let(:pool) do
      described_class.new(authorized_client)
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

        it 'disposes of the old session and returns a new one' do
          expect(checked_out_session).not_to be(old_session_a)
          expect(checked_out_session).not_to be(old_session_b)
          expect(pool.instance_variable_get(:@queue)).to be_empty
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

      it 'disposes of the old sessions instead of adding them to the pool' do
        expect(pool.checkout).not_to be(old_session_a)
        expect(pool.checkout).not_to be(old_session_b)
        expect(pool.instance_variable_get(:@queue)).to be_empty
      end
    end
  end

  describe '#end_sessions', if: sessions_enabled? do

    let(:pool) do
      described_class.create(client)
    end

    let!(:session_a) do
      pool.checkout
    end

    let!(:session_b) do
      pool.checkout
    end

    let(:client) do
      authorized_client.with(heartbeat_frequency: 100).tap do |cl|
        cl.subscribe(Mongo::Monitoring::COMMAND, subscriber)
      end
    end

    let(:subscriber) do
      EventSubscriber.new
    end

    before do
      client.database.command(ping: 1)
      pool.checkin(session_a)
      pool.checkin(session_b)
      pool.end_sessions
    end

    after do
      client.close
    end

    let(:end_sessions_command) do
      subscriber.started_events.find { |c| c.command_name == :endSessions}
    end

    it 'sends the endSessions command with all the session ids' do
      expect(end_sessions_command.command[:ids]).to include(BSON::Document.new(session_a.session_id))
      expect(end_sessions_command.command[:ids]).to include(BSON::Document.new(session_b.session_id))
    end

    context 'when talking to a mongos', if: sessions_enabled? && sharded? do

      it 'sends the endSessions command with all the session ids' do
        expect(end_sessions_command.command[:ids]).to include(BSON::Document.new(session_a.session_id))
        expect(end_sessions_command.command[:ids]).to include(BSON::Document.new(session_b.session_id))
        expect(end_sessions_command.command[:$clusterTime]).to eq(client.cluster.cluster_time)
      end
    end

    context 'when the number of ids is larger than 10_000' do

      before do
        queue = []
        10_001.times do |i|
          queue << double('session', session_id: i)
        end
        pool.instance_variable_set(:@queue, queue)
        expect(Mongo::Operation::Commands::Command).to receive(:new).at_least(:twice)
      end

      let(:end_sessions_commands) do
        subscriber.started_events.select { |c| c.command_name == :endSessions}
      end

      it 'sends the command more than once' do
        pool.end_sessions
        # expect(end_sessions_commands.size).to eq(2)
        # expect(end_sessions_commands[0].command[:ids]).to eq([*0...10_000])
        # expect(end_sessions_commands[1].command[:ids]).to eq([10_000])
      end
    end
  end
end

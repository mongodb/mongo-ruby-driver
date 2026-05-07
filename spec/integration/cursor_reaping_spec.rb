# frozen_string_literal: true

require 'spec_helper'

describe 'Cursor reaping' do
  # JRuby does reap cursors but GC.start does not force GC to run like it does
  # in MRI, I don't currently know how to force GC to run in JRuby
  require_mri

  # Uncomment for debugging this test.
  #   around(:all) do |example|
  #     saved_level = Mongo::Logger.logger.level
  #     Mongo::Logger.logger.level = Logger::DEBUG
  #     begin
  #       example.run
  #     ensure
  #       Mongo::Logger.logger.level = saved_level
  #     end
  #   end

  let(:subscriber) { Mrss::EventSubscriber.new }

  let(:client) do
    authorized_client.with(max_pool_size: 10).tap do |client|
      client.subscribe(Mongo::Monitoring::COMMAND, subscriber)
    end
  end

  let(:collection) { client['cursor_reaping_spec'] }

  before do
    data = [ { a: 1 } ] * 10
    authorized_client['cursor_reaping_spec'].delete_many
    authorized_client['cursor_reaping_spec'].insert_many(data)
  end

  context 'a no-timeout cursor' do
    # this let block is a kludge to avoid copy pasting all of this code
    let(:cursor_id_and_kill_event) do
      expect(Mongo::Operation::KillCursors).to receive(:new).at_least(:once).and_call_original

      cursor_ids = abandon_cursors

      cursor_ids.each do |cursor_id|
        expect(cursor_id).to be_a(Integer)
        expect(cursor_id > 0).to be true
      end

      GC.start
      sleep 1

      # force periodic executor to run because its frequency is not configurable
      client.cluster.instance_variable_get(:@periodic_executor).execute

      started_event = subscriber.started_events.detect do |event|
        event.command['killCursors']
      end
      started_event.should_not be_nil

      found_cursor_id = nil
      started_event = subscriber.started_events.detect do |event|
        found = false
        if event.command['killCursors']
          cursor_ids.each do |cursor_id|
            next unless event.command['cursors'].map { |c| Utils.int64_value(c) }.include?(cursor_id)

            found_cursor_id = cursor_id
            found = true
            break
          end
        end
        found
      end

      p subscriber.started_events if started_event.nil?

      started_event.should_not be_nil

      succeeded_event = subscriber.succeeded_events.detect do |event|
        event.command_name == 'killCursors' && event.request_id == started_event.request_id
      end

      expect(succeeded_event).not_to be_nil

      expect(succeeded_event.reply['ok']).to eq 1

      [ found_cursor_id, succeeded_event ]
    end

    it 'reaps nothing when we do not query' do
      # this is a base line test to ensure that the reaps in the other test
      # aren't done on some global cursor
      expect(Mongo::Operation::KillCursors).not_to receive(:new)

      # just the scope, no query is happening
      collection.find.batch_size(2).no_cursor_timeout

      events = subscriber.started_events.select do |event|
        event.command['killCursors']
      end

      expect(events).to be_empty
    end

    def abandon_cursors
      [].tap do |cursor_ids|
        # scopes are weird, having this result in a let block
        # makes it not garbage collected
        10.times do
          scope = collection.find.batch_size(2).no_cursor_timeout

          # Begin iteration, creating the cursor
          scope.each.first

          scope.cursor.should_not be_nil
          cursor_ids << scope.cursor.id
        end
      end
    end

    it 'is reaped' do
      cursor_id_and_kill_event
    end

    it 'is really killed' do
      cursor_id, event = cursor_id_and_kill_event

      expect(event.reply['cursorsKilled']).to eq([ cursor_id ])
      expect(event.reply['cursorsNotFound']).to be_empty
      expect(event.reply['cursorsAlive']).to be_empty
      expect(event.reply['cursorsUnknown']).to be_empty
    end
  end

  context 'load-balanced topology' do
    require_topology :load_balanced

    let(:pool_size) { 3 }

    let(:client) do
      authorized_client.with(max_pool_size: pool_size, wait_queue_timeout: 2)
    end

    let(:collection) { client['cursor_reaper_lb_leak'] }

    before do
      authorized_client['cursor_reaper_lb_leak'].drop
      200.times { |i| authorized_client['cursor_reaper_lb_leak'].insert_one(name: "doc_#{i}") }
    end

    def kill_checked_out_sockets(pool)
      pool.instance_variable_get(:@checked_out_connections).each do |conn|
        sock = conn.instance_variable_get(:@socket)
        raw = sock&.instance_variable_get(:@tcp_socket) ||
              sock&.instance_variable_get(:@socket)
        raw&.close unless raw.nil? || raw.closed?
      rescue StandardError
      end
    end

    it 'releases pinned connections when the socket dies before killCursors runs' do
      server = client.cluster.next_primary
      pool = server.pool

      # Open pool_size cursors with cursor_id != 0. batch_size: 1 ensures the
      # server never exhausts the cursor in the first batch, so each cursor pins
      # its connection until the reaper runs.
      pool_size.times do
        scope = collection.find({}, batch_size: 1)
        scope.each.first
      end

      # All connections should be pinned (checked out) and none available.
      expect(pool.available_count).to eq(0)
      expect(pool.state[:checked_out_connections]).to eq(pool_size)

      # Simulate a network failure by force-closing the underlying TCP sockets
      # while the connections are still pinned to their open cursors.
      kill_checked_out_sockets(pool)

      # The block-local `scope` variables are now eligible for GC. Force
      # collection so finalizers run and KillSpecs are enqueued.
      GC.start
      sleep 1

      # Force the cursor reaper (via the periodic executor) to process the
      # queued KillSpecs. Without the fix, execute_with_connection raises
      # SocketError on each dead connection and check_in is never called;
      # the rescue here prevents that exception from failing the test for the
      # wrong reason.
      begin
        client.cluster.trigger_periodic_executor!
      rescue StandardError
        nil
      end

      # Every pinned connection should have been checked back into the pool,
      # even though the killCursors command failed on the dead socket. Without
      # the fix, @checked_out_connections still holds all pool_size connections
      # and this expectation fails.
      expect(pool.state[:checked_out_connections]).to eq(0)
    end
  end
end

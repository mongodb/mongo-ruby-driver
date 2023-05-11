# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe 'Cursor reaping' do
  # JRuby does reap cursors but GC.start does not force GC to run like it does
  # in MRI, I don't currently know how to force GC to run in JRuby
  require_mri

# Uncomment for debugging this test.
=begin
  around(:all) do |example|
    saved_level = Mongo::Logger.logger.level
    Mongo::Logger.logger.level = Logger::DEBUG
    begin
      example.run
    ensure
      Mongo::Logger.logger.level = saved_level
    end
  end
=end

  let(:subscriber) { Mrss::EventSubscriber.new }

  let(:client) do
    authorized_client.tap do |client|
      client.subscribe(Mongo::Monitoring::COMMAND, subscriber)
    end
  end

  let(:collection) { client['cursor_reaping_spec'] }

  before do
    data = [{a: 1}] * 10
    authorized_client['cursor_reaping_spec'].delete_many
    authorized_client['cursor_reaping_spec'].insert_many(data)
  end

  context 'a no-timeout cursor' do
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

          scope.cursor.should_not be nil
          cursor_ids << scope.cursor.id
        end
      end
    end

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
      client.cluster.instance_variable_get('@periodic_executor').execute

      started_event = subscriber.started_events.detect do |event|
        event.command['killCursors']
      end
      started_event.should_not be nil

      found_cursor_id = nil
      started_event = subscriber.started_events.detect do |event|
        found = false
        if event.command['killCursors']
          cursor_ids.each do |cursor_id|
            if event.command['cursors'].map { |c| Utils.int64_value(c) }.include?(cursor_id)
              found_cursor_id = cursor_id
              found = true
              break
            end
          end
        end
        found
      end

      if started_event.nil?
        p subscriber.started_events
      end

      started_event.should_not be nil

      succeeded_event = subscriber.succeeded_events.detect do |event|
        event.command_name == 'killCursors' && event.request_id == started_event.request_id
      end

      expect(succeeded_event).not_to be_nil

      expect(succeeded_event.reply['ok']).to eq 1

      [found_cursor_id, succeeded_event]
    end

    it 'is reaped' do
      cursor_id_and_kill_event
    end

    context 'newer servers' do
      min_server_fcv '3.2'

      it 'is really killed' do
        cursor_id, event = cursor_id_and_kill_event

        expect(event.reply['cursorsKilled']).to eq([cursor_id])
        expect(event.reply['cursorsNotFound']).to be_empty
        expect(event.reply['cursorsAlive']).to be_empty
        expect(event.reply['cursorsUnknown']).to be_empty
      end
    end
  end
end

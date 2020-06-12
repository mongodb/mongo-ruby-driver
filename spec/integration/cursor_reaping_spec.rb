require 'spec_helper'

describe 'Cursor reaping' do
  # JRuby does reap cursors but GC.start does not force GC to run like it does
  # in MRI, I don't currently know how to force GC to run in JRuby
  only_mri

  let(:subscriber) { EventSubscriber.new }

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

    # this let block is a kludge to avoid copy pasting all of this code
    let(:cursor_id_and_kill_event) do
      expect(Mongo::Operation::KillCursors).to receive(:new).at_least(:once).and_call_original

      cursor_id = nil

      # scopes are weird, having this result in a let block
      # makes it not garbage collected
      2.times do
        scope = collection.find.batch_size(2).no_cursor_timeout

        # there is no API for retrieving the cursor
        scope.each.first
        # and keep the first cursor
        cursor_id ||= scope.instance_variable_get('@cursor').id
      end

      expect(cursor_id).to be_a(Integer)
      expect(cursor_id > 0).to be true

      GC.start

      # force periodic executor to run because its frequency is not configurable
      client.cluster.instance_variable_get('@periodic_executor').execute

      started_event = subscriber.started_events.detect do |event|
        event.command['killCursors'] &&
        event.command['cursors'].map { |c| Utils.int64_value(c) }.include?(cursor_id)
      end

      expect(started_event).not_to be_nil

      succeeded_event = subscriber.succeeded_events.detect do |event|
        event.command_name == 'killCursors' && event.request_id == started_event.request_id
      end

      expect(succeeded_event).not_to be_nil

      expect(succeeded_event.reply['ok']).to eq 1

      [cursor_id, succeeded_event]
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

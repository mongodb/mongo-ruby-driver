require 'spec_helper'

describe 'Cursor reaping' do
  # JRuby does reap cursors but GC.start does not force GC to run like it does
  # in MRI, I don't currently know how to force GC to run in JRuby
  only_mri

  let(:client) { subscribed_client }
  let(:collection) { client['cursor_reaping_spec'] }

  before(:all) do
    data = [{a: 1}] * 10
    ClientRegistry.instance.global_client('subscribed')['cursor_reaping_spec'].insert_many(data)
  end

  context 'a no-timeout cursor' do
    before do
      EventSubscriber.clear_events!
    end

    let(:events) do
      EventSubscriber.started_events.select do |event|
        event.command['killCursors']
      end
    end

    it 'reaps nothing when we do not query' do
      # this is a base line test to ensure that the reaps in the other test
      # aren't done on some global cursor
      expect(Mongo::Operation::KillCursors).not_to receive(:new)

      # just the scope, no query is happening
      collection.find.batch_size(2).no_cursor_timeout

      expect(events).to be_empty
    end

    it 'is reaped' do
      expect(Mongo::Operation::KillCursors).to receive(:new).at_least(:once).and_call_original

      # scopes are weird, having this result in a let block
      # makes it not garbage collected
      2.times { collection.find.batch_size(2).no_cursor_timeout.first }

      GC.start

      # force periodic executor to run because its frequency is not configurable
      client.cluster.instance_variable_get('@periodic_executor').execute

      expect(events).not_to be_empty
    end
  end
end

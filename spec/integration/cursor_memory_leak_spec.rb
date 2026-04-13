# frozen_string_literal: true

require 'spec_helper'

describe 'Cursor memory leak - RUBY-3669' do
  # Regression test: when batch_size < limit, each find/each iteration
  # used to leak a Mongo::Client via the GC finalizer -> KillSpec -> Session
  # -> Session#@client chain. The fix clears @client in Session#end_session
  # and ensures CursorReaper discards stale KillSpecs cleanly.

  # ObjectSpace is MRI-specific; on JRuby GC.start is not deterministic
  require_mri

  let(:collection_name) { 'cursor_memory_leak_spec' }
  let(:collection) { authorized_client[collection_name] }

  before do
    collection.delete_many
    collection.insert_many([ { a: 1 }, { a: 2 }, { a: 3 } ])
  end

  it 'does not leak Mongo::Client objects when batch_size < limit' do
    # Warm up: run once so any one-time initialization clients are created
    # before we start counting.
    collection.find(nil, batch_size: 2, limit: 3).to_a

    GC.start
    GC.start
    GC.start
    sleep (Mongo::Cluster::CursorReaper::FREQUENCY * 2) + 1

    client_count_before = ObjectSpace.each_object(Mongo::Client).count

    10.times do
      collection.find(nil, batch_size: 2, limit: 3).to_a
    end

    # Give the GC and the periodic cursor reaper time to process finalizers
    # and discard stale KillSpecs.
    GC.start
    GC.start
    GC.start
    sleep (Mongo::Cluster::CursorReaper::FREQUENCY * 2) + 1

    client_count_after = ObjectSpace.each_object(Mongo::Client).count

    expect(client_count_after).to be <= client_count_before,
                                  "Expected Mongo::Client count to stay at #{client_count_before} but got #{client_count_after} — possible memory leak"
  end
end

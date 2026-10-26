# frozen_string_literal: true

require 'lite_spec_helper'

# Functional smoke test for Atlas Stream Processing.
#
# Skipped unless the `MONGODB_STREAM_PROCESSING_URI` env var is set to a
# workspace endpoint (atlas-stream-*.<region>.a.query.mongodb{,-stage}.net)
# with valid credentials.
#
# Exercises the full lifecycle: create -> start -> stats -> sample -> stop -> drop.
describe 'Atlas Stream Processing lifecycle' do
  before do
    uri = ENV['MONGODB_STREAM_PROCESSING_URI']
    skip 'MONGODB_STREAM_PROCESSING_URI is not configured' if uri.nil? || uri.empty?

    unless Mongo::StreamProcessing::Client.workspace_uri?(uri)
      skip "MONGODB_STREAM_PROCESSING_URI=#{uri.inspect} is not a workspace endpoint"
    end
  end

  let(:uri) { ENV['MONGODB_STREAM_PROCESSING_URI'] }
  let(:client) { Mongo::StreamProcessing::Client.new(uri) }
  let(:processors) { client.stream_processors }
  let(:name) { "rubydriver_test_#{BSON::ObjectId.new}" }

  after do
    # Best-effort cleanup. The drop will fail silently if the processor was
    # never created or has already been dropped, which is the safe behavior.
    begin
      processors.get(name).drop
    rescue Mongo::Error
      # ignored
    end
    client.close
  end

  it 'runs the full lifecycle' do
    pipeline = [
      { '$source' => { 'connectionName' => 'sample_stream_solar' } },
      { '$emit' => { 'connectionName' => '__testLog', 'topic' => 'ruby-driver-demo' } }
    ]

    # create
    processors.create(name, pipeline)
    info = processors.get_info(name)
    expect(info.name).to eq(name)
    expect(info.state).to eq('CREATED').or(eq('VALIDATING')).or(eq('CREATING'))

    # start
    processor = processors.get(name)
    processor.start

    # Wait for STARTED
    deadline = Time.now + 30
    while Time.now < deadline
      state = processors.get_info(name).state
      break if state == 'STARTED'

      sleep 0.5
    end
    expect(processors.get_info(name).state).to eq('STARTED')

    # stats
    stats = processor.stats
    expect(stats).to be_a(Hash)

    # sample: open + fetch one batch
    opened = processor.samples(limit: 5)
    expect(opened.cursor_id).to be > 0
    expect(opened.documents).to eq([])

    batch = processor.samples(cursor_id: opened.cursor_id, batch_size: 5)
    # cursor_id may be 0 if the stream has nothing yet — both are valid.
    expect(batch.cursor_id).to be >= 0

    # stop + drop
    processor.stop
    processor.drop
  end
end

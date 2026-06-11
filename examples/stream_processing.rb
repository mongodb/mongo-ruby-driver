# frozen_string_literal: true

# Demonstrates the full lifecycle of an Atlas Stream Processing (ASP) stream
# processor using Mongo::StreamProcessing::Client. It creates, starts, samples,
# stops, and drops a processor.
#
# Requirements:
#   - An Atlas Stream Processing workspace with a hostname matching the pattern
#     atlas-stream-<workspaceId>-<suffix>.<region>.a.query.mongodb.net
#     (or .mongodb-<env>.net for staging).
#   - A user with the `atlasAdmin` role.
#   - Two connections registered in the workspace:
#       - `sample_stream_solar`  (built-in sample source)
#       - `__testLog`            (built-in test sink)
#
# Run with:
#
#   MONGODB_STREAM_PROCESSING_URI='mongodb://user:pass@atlas-stream-….a.query.mongodb.net/' \
#       bundle exec ruby examples/stream_processing.rb

require 'mongo'

uri = ENV['MONGODB_STREAM_PROCESSING_URI']
if uri.nil? || uri.empty?
  warn 'This example requires an Atlas Stream Processing workspace endpoint.'
  warn 'Set MONGODB_STREAM_PROCESSING_URI to the workspace connection string.'
  exit 1
end

unless Mongo::StreamProcessing::Client.workspace_uri?(uri)
  warn 'MONGODB_STREAM_PROCESSING_URI does not look like a workspace endpoint.'
  warn 'Expected: atlas-stream-*.<region>.a.query.mongodb.net (or .mongodb-stage.net for staging)'
  exit 1
end

client = Mongo::StreamProcessing::Client.new(uri)
processors = client.stream_processors
name = "rubydriver_demo_#{BSON::ObjectId.new}"

puts "Workspace: #{uri}"
puts "Processor: #{name}"
puts

created = false
begin
  pipeline = [
    { '$source' => { 'connectionName' => 'sample_stream_solar' } },
    { '$emit' => { 'connectionName' => '__testLog', 'topic' => 'ruby-driver-demo' } }
  ]

  # 1. create
  puts "[1/6] create(#{name})"
  processors.create(name, pipeline)
  created = true
  info = processors.get_info(name)
  puts "      state=#{info.state}"
  puts

  # 2. start
  puts '[2/6] start()'
  processor = processors.get(name)
  processor.start
  deadline = Time.now + 30
  state = processors.get_info(name).state
  while state != 'STARTED' && Time.now < deadline
    sleep 0.5
    state = processors.get_info(name).state
  end
  puts "      state=#{state}"
  puts
  raise "processor did not reach STARTED within 30s (got #{state})" if state != 'STARTED'

  # 3. stats
  puts '[3/6] stats()'
  stats = processor.stats
  puts "      #{stats.inspect}"
  puts

  # 4. samples
  puts '[4/6] samples()'
  opened = processor.samples(limit: 5)
  puts "      open  cursor_id=#{opened.cursor_id} docs=#{opened.documents.size}"

  unless opened.exhausted?
    sleep 2 # give the stream a moment to produce something
    batch = processor.samples(cursor_id: opened.cursor_id, batch_size: 5)
    puts "      batch cursor_id=#{batch.cursor_id} docs=#{batch.documents.size}"
    batch.documents.each_with_index do |doc, i|
      puts "          [#{i}] #{doc.inspect}"
    end
  end
  puts

  # 5. stop
  puts '[5/6] stop()'
  processor.stop
  puts "      state=#{processors.get_info(name).state}"
  puts

  # 6. drop
  puts '[6/6] drop()'
  processor.drop
  puts '      dropped'
  puts

  puts 'OK.'
rescue StandardError => e
  warn ''
  warn "FAILED: #{e.class}: #{e.message}"
  warn e.backtrace.first(15).join("\n") if e.backtrace
  if created
    begin
      processors.get(name).drop
      warn "(cleaned up processor #{name})"
    rescue StandardError
      # best-effort cleanup
    end
  end
  exit 1
ensure
  client.close
end

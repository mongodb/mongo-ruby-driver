# frozen_string_literal: true

require 'mongo'
require 'json'

$open_connections = 0
$heartbeats_count = 0
$total_heartbeat_time = 0
$commands_count = 0
$total_command_time = 0


class CommandMonitor
  def started(event)
  end

  def failed(event)
    $commands_count += 1
    $total_command_time += event.duration
  end

  def succeeded(event)
    $commands_count += 1
    $total_command_time += event.duration
  end
end

class HeartbeatMonitor
  def started(event)
  end

  def succeeded(event)
    $heartbeats_count += 1
    $total_heartbeat_time += event.duration
  end

  def failed(event)
    $heartbeats_count += 1
    $total_heartbeat_time += event.duration
  end
end

class PoolMonitor
  def published(event)
    case event
    when Mongo::Monitoring::Event::Cmap::ConnectionCreated
      $open_connections += 1
    when Mongo::Monitoring::Event::Cmap::ConnectionClosed
      $open_connections -= 1
    end
  end
end

command_monitor = CommandMonitor.new
heartbeat_monitor = HeartbeatMonitor.new
pool_monitor = PoolMonitor.new

sdam_proc = proc do |client|
  client.subscribe(Mongo::Monitoring::COMMAND, command_monitor)
  client.subscribe(Mongo::Monitoring::SERVER_HEARTBEAT, heartbeat_monitor)
  client.subscribe(Mongo::Monitoring::CONNECTION_POOL, pool_monitor)
end

$client = Mongo::Client.new(ENV['MONGODB_URI'], sdam_proc: sdam_proc)

puts 'Connecting'
$client.use('lambda_test').database.list_collections
puts 'Connected'

def reset_counters
  $heartbeats_count = 0
  $total_heartbeat_time = 0
  $commands_count = 0
  $total_command_time = 0
  $open_connections = 0
end

def average_heartbeat_time
  if $heartbeats_count == 0
    0
  else
    $total_heartbeat_time / $heartbeats_count
  end
end

def average_command_time
  if $commands_count == 0
    0
  else
    $total_command_time / $commands_count
  end
end

def lambda_handler(event:, context:)
  db = $client.use('lambda_test')
  collection = db[:test_collection]
  result = collection.insert_one({ name: 'test' })
  collection.delete_one({ _id: result.inserted_id })
  response = {
    average_heartbeat_time: average_heartbeat_time,
    average_command_time: average_command_time,
    heartbeats_count: $heartbeats_count,
    open_connections: $open_connections,
  }.to_json
  reset_counters
  puts "Response: #{response}"

  {
    statusCode: 200,
    body: response
  }
end


lambda_handler(event: nil, context: nil)

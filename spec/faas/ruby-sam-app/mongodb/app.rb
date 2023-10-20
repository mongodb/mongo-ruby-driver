# frozen_string_literal: true

require 'mongo'
require 'json'

class StatsAggregator

  def initialize
    @open_connections = 0
    @heartbeats_count = 0
    @total_heartbeat_time = 0
    @commands_count = 0
    @total_command_time = 0
  end

  def add_command(duration)
    @commands_count += 1
    @total_command_time += duration
  end

  def add_heartbeat(duration)
    @heartbeats_count += 1
    @total_heartbeat_time += duration
  end

  def add_connection
    @open_connections += 1
  end

  def remove_connection
    @open_connections -= 1
  end

  def average_heartbeat_time
    if @heartbeats_count == 0
      0
    else
      @total_heartbeat_time / @heartbeats_count
    end
  end

  def average_command_time
    if @commands_count == 0
      0
    else
      @total_command_time / @commands_count
    end
  end

  def reset
    @open_connections = 0
    @heartbeats_count = 0
    @total_heartbeat_time = 0
    @commands_count = 0
    @total_command_time = 0
  end

  def result
    {
      average_heartbeat_time: average_heartbeat_time,
      average_command_time: average_command_time,
      heartbeats_count: @heartbeats_count,
      open_connections: @open_connections,
    }
  end
end

class CommandMonitor

  def initialize(stats_aggregator)
    @stats_aggregator = stats_aggregator
  end

  def started(event); end

  def failed(event)
    @stats_aggregator.add_command(event.duration)
  end

  def succeeded(event)
    @stats_aggregator.add_command(event.duration)
  end
end

class HeartbeatMonitor

  def initialize(stats_aggregator)
    @stats_aggregator = stats_aggregator
  end

  def started(event); end

  def succeeded(event)
    @stats_aggregator.add_heartbeat(event.duration)
  end

  def failed(event)
    @stats_aggregator.add_heartbeat(event.duration)
  end
end

class PoolMonitor

  def initialize(stats_aggregator)
    @stats_aggregator = stats_aggregator
  end

  def published(event)
    case event
    when Mongo::Monitoring::Event::Cmap::ConnectionCreated
      @stats_aggregator.add_connection
    when Mongo::Monitoring::Event::Cmap::ConnectionClosed
      @stats_aggregator.remove_connection
    end
  end
end

$stats_aggregator = StatsAggregator.new

command_monitor = CommandMonitor.new($stats_aggregator)
heartbeat_monitor = HeartbeatMonitor.new($stats_aggregator)
pool_monitor = PoolMonitor.new($stats_aggregator)

sdam_proc = proc do |client|
  client.subscribe(Mongo::Monitoring::COMMAND, command_monitor)
  client.subscribe(Mongo::Monitoring::SERVER_HEARTBEAT, heartbeat_monitor)
  client.subscribe(Mongo::Monitoring::CONNECTION_POOL, pool_monitor)
end

puts 'Connecting'
$client = Mongo::Client.new(ENV['MONGODB_URI'], sdam_proc: sdam_proc)
# Populate the connection pool
$client.use('lambda_test').database.list_collections
puts 'Connected'

def lambda_handler(event:, context:)
  db = $client.use('lambda_test')
  collection = db[:test_collection]
  result = collection.insert_one({ name: 'test' })
  collection.delete_one({ _id: result.inserted_id })
  response = $stats_aggregator.result.to_json
  $stats_aggregator.reset
  puts "Response: #{response}"

  {
    statusCode: 200,
    body: response
  }
end

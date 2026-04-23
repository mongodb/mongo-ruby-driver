# frozen_string_literal: true

# Connection pool fairness harness.
#
# Drives the pool under heavy over-subscription and measures per-thread
# service counts and wait-time distribution. Originally written to diagnose
# RUBY-3364 ("Mongo Connection Pool should serve queries in FIFO manner"),
# and kept as a permanent regression/fairness check.
#
# A fair pool, given N threads and a pool size of M (N >> M), should serve
# every thread roughly the same number of times over a long enough run.
# With the pre-fix code, 5 "lucky" threads held the connections for the
# entire run and the other 195 threads starved and hit the wait_timeout
# exactly once each. A healthy pool shows min/median/max per-thread counts
# within ~1% of each other.
#
# Usage:
#   MONGODB_URI="mongodb://..." bundle exec ruby profile/connection_pool_fairness.rb
#
# Tunable via environment:
#   MONGODB_URI         cluster URI (default: local replica set on 27017-9)
#   POOL_SIZE           max_pool_size (default: 5)
#   THREADS             concurrent worker threads (default: 200)
#   DURATION_SEC        how long to run (default: 30)
#   WAIT_TIMEOUT        pool wait_queue_timeout in seconds (default: 10)

require 'mongo'
require 'logger'

MONGO_URI = ENV.fetch('MONGODB_URI',
                      'mongodb://localhost:27017,localhost:27018,localhost:27019/?replicaSet=replset')
POOL_SIZE     = Integer(ENV.fetch('POOL_SIZE',     '5'))
THREADS       = Integer(ENV.fetch('THREADS',       '200'))
DURATION_SEC  = Integer(ENV.fetch('DURATION_SEC',  '30'))
WAIT_TIMEOUT  = Float(ENV.fetch('WAIT_TIMEOUT', '10'))
DB_NAME       = 'ruby_3364'
COLL_NAME     = 'probe'

Mongo::Logger.logger       = Logger.new(File::NULL)
Mongo::Logger.logger.level = Logger::FATAL

def now_us
  (Process.clock_gettime(Process::CLOCK_MONOTONIC) * 1_000_000).to_i
end

client = Mongo::Client.new(
  MONGO_URI,
  database: DB_NAME,
  max_pool_size: POOL_SIZE,
  min_pool_size: POOL_SIZE,
  wait_queue_timeout: WAIT_TIMEOUT,
  logger: Mongo::Logger.logger
)

# Seed the collection so first() has something to return.
client[COLL_NAME].drop
client[COLL_NAME].insert_one(x: 1)

results = Queue.new
stop_at = now_us + (DURATION_SEC * 1_000_000)
start_barrier = Queue.new

threads = Array.new(THREADS) do |i|
  Thread.new do
    start_barrier.pop
    while now_us < stop_at
      t1 = now_us
      err = nil
      begin
        Mongo::QueryCache.uncached do
          client[COLL_NAME].find.first
        end
      rescue StandardError => e
        err = e.class.name
      end
      t2 = now_us
      results << [ i, t1, t2, t2 - t1, err ]
    end
  end
end

# Release all threads at once to get maximum contention.
THREADS.times { start_barrier << :go }

threads.each(&:join)
client.close

# Drain queue into array.
rows = []
rows << results.pop until results.empty?

total    = rows.size
errors   = rows.count { |r| r[4] }
timeouts = rows.count { |r| r[4] == 'Mongo::Error::ConnectionCheckOutTimeout' }
puts "Total ops:          #{total}"
puts "Errors (any):       #{errors}  (#{format('%.4f', 100.0 * errors / total)}%)"
puts "Checkout timeouts:  #{timeouts}  (#{format('%.4f', 100.0 * timeouts / total)}%)"

# Wait-time band histogram: buckets of 1s up to 11s.
buckets = Array.new(12, 0)
rows.each do |_, _, _, dur, _|
  idx = [ dur / 1_000_000, 11 ].min
  buckets[idx] += 1
end
puts
puts 'Wait-time band histogram (seconds):'
buckets.each_with_index do |n, i|
  label = (i == 11) ? '>10s' : "#{i}-#{i + 1}s"
  bar = '#' * [ (n.to_f / total * 400).to_i, 80 ].min
  puts "  #{label.rjust(6)}: #{n.to_s.rjust(8)}  #{bar}"
end

# Fine-grained banding in the 0-10s range (by 500ms buckets) to detect the
# reporter's 2s/4s/6s/8s pattern.
puts
puts 'Fine band histogram (500ms buckets, 0-10s):'
fine = Array.new(21, 0)
rows.each do |_, _, _, dur, _|
  idx = [ dur / 500_000, 20 ].min
  fine[idx] += 1
end
fine.each_with_index do |n, i|
  lo = i * 0.5
  hi = (i + 1) * 0.5
  bar = '#' * [ (n.to_f / total * 400).to_i, 80 ].min
  puts "  #{format('%.1f-%.1fs', lo, hi).rjust(10)}: #{n.to_s.rjust(8)}  #{bar}"
end

# Per-thread op counts — are any threads starved?
per_thread = Hash.new(0)
per_thread_errors = Hash.new(0)
rows.each do |r|
  per_thread[r[0]] += 1
  per_thread_errors[r[0]] += 1 if r[4]
end

# Make sure every thread id appears (some may have zero completions)
THREADS.times do |i|
  per_thread[i] ||= 0
  per_thread_errors[i] ||= 0
end

counts = per_thread.values.sort
puts
puts 'Per-thread op count distribution:'
puts "  min=#{counts.first}, p10=#{counts[counts.size / 10]}, " \
     "median=#{counts[counts.size / 2]}, p90=#{counts[counts.size * 9 / 10]}, " \
     "max=#{counts.last}"
puts "  threads with 0 ops:    #{counts.count(&:zero?)}"
puts "  threads with <10 ops:  #{counts.count { |c| c < 10 }}"
puts "  threads with >100 ops: #{counts.count { |c| c > 100 }}"
puts "  threads with >1000 ops:#{counts.count { |c| c > 1000 }}"
puts

# Top 5 and bottom 5 threads by count
ranked = per_thread.sort_by { |_, v| v }
puts 'Bottom 10 threads by op count:'
ranked.first(10).each do |tid, n|
  puts "  thread #{tid.to_s.rjust(3)}: #{n.to_s.rjust(6)} ops, #{per_thread_errors[tid]} timeouts"
end
puts
puts 'Top 10 threads by op count:'
ranked.last(10).each do |tid, n|
  puts "  thread #{tid.to_s.rjust(3)}: #{n.to_s.rjust(6)} ops, #{per_thread_errors[tid]} timeouts"
end

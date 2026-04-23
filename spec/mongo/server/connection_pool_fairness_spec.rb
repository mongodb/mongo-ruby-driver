# frozen_string_literal: true

require 'spec_helper'

# RUBY-3364: regression for thread starvation under pool over-subscription.
# Drives a small pool from many threads and asserts that no thread is
# served materially less often than any other.
describe 'Mongo::Server::ConnectionPool fairness', retry: 3 do
  let(:client) do
    authorized_client.with(
      max_pool_size: 3,
      min_pool_size: 3,
      wait_queue_timeout: 5
    )
  end

  let(:coll) { client['ruby_3364_fairness'] }

  before do
    coll.drop
    coll.insert_one(x: 1)
  end

  after do
    client.close
  end

  it 'serves every thread at roughly the same rate and does not time out' do
    threads_count = 30
    duration_sec = 3
    stop_at = Mongo::Utils.monotonic_time + duration_sec
    start_barrier = Queue.new
    counts = Array.new(threads_count, 0)
    timeouts = Array.new(threads_count, 0)

    threads = Array.new(threads_count) do |i|
      Thread.new do
        start_barrier.pop
        while Mongo::Utils.monotonic_time < stop_at
          begin
            Mongo::QueryCache.uncached { coll.find.first }
            counts[i] += 1
          rescue Mongo::Error::ConnectionCheckOutTimeout
            timeouts[i] += 1
          end
        end
      end
    end

    threads_count.times { start_barrier << :go }
    threads.each(&:join)

    total_timeouts = timeouts.sum
    expect(total_timeouts).to eq(0),
                              "expected zero checkout timeouts, got #{total_timeouts} " \
                              "(per-thread: #{timeouts.inspect})"

    min_count = counts.min
    max_count = counts.max
    expect(min_count).to be > 0, 'at least one thread was never served'
    ratio = min_count.to_f / max_count
    expect(ratio).to be >= 0.5,
                     "unfair distribution: min=#{min_count}, max=#{max_count}, ratio=#{ratio.round(3)} " \
                     "(counts: #{counts.inspect})"
  end
end

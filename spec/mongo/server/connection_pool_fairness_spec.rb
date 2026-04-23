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

  # RUBY-3364: regression for lost-wakeup in the anti-barging gate.
  #
  # The anti-barging change makes a thread arriving at the gate wait whenever
  # `@size_waiters > 0`. That counter is only decremented after `@size_cv.wait`
  # returns and the waiter re-acquires the lock. During the brief window
  # between signal delivery and re-acquisition, a newcomer can observe a
  # non-zero `@size_waiters` and enter the wait. If the newcomer enters while
  # capacity is available (e.g., a batch of check-ins fired more signals than
  # there were waiters), and no further signal arrives, the newcomer times
  # out despite capacity being available — a classic lost wakeup.
  #
  # This test forces the race window deterministically by instrumenting the
  # pool's `@size_cv.wait` to yield the lock long enough for a newcomer to
  # enter and queue up. The fix signals the next waiter when a successful
  # waiter leaves the queue.
  describe 'lost-wakeup at the size gate' do
    let(:client) do
      authorized_client.with(
        max_pool_size: 2,
        min_pool_size: 2,
        wait_queue_timeout: 2
      )
    end

    let(:coll) { client['ruby_3364_lostwakeup'] }

    before do
      coll.drop
      coll.insert_one(x: 1)
    end

    after do
      client.close
    end

    it 'does not strand a newcomer that enters during a waiter wake-up window' do
      pool = client.cluster.next_primary.pool
      size_cv = pool.instance_variable_get(:@size_cv)
      pool_lock = pool.instance_variable_get(:@lock)

      # Instrument @size_cv.wait so the FIRST wake-up releases the lock
      # for long enough that a newcomer can grab it and observe the stale
      # @size_waiters > 0 state. Subsequent waits are not instrumented.
      injected = false
      inject_mutex = Mutex.new
      original_wait = size_cv.method(:wait)
      size_cv.define_singleton_method(:wait) do |timeout = nil|
        result = original_wait.call(timeout)
        should_inject = false
        inject_mutex.synchronize do
          unless injected
            injected = true
            should_inject = true
          end
        end
        if should_inject
          # Open the race window: release the pool lock long enough for a
          # newcomer thread to observe the stale `@size_waiters > 0` state
          # before we decrement it.
          pool_lock.unlock
          begin
            sleep 0.3
          ensure
            pool_lock.lock
          end
        end
        result
      end

      begin
        # Fill the pool.
        holders_ready = Queue.new
        release_holders = Queue.new
        holders = Array.new(2) do
          Thread.new do
            conn = pool.check_out
            holders_ready << :ready
            release_holders.pop
            pool.check_in(conn)
          end
        end
        2.times { holders_ready.pop }

        # Queue a single waiter. After it acquires, it holds until we
        # explicitly release — no check-in fires from this thread until
        # the end of the test.
        waiter_release = Queue.new
        waiter = Thread.new do
          conn = pool.check_out
          waiter_release.pop
          pool.check_in(conn)
        end
        sleep 0.2 # allow waiter to block on @size_cv.wait

        # Release BOTH holders. Two @size_cv.signal calls fire; only one
        # waiter exists. When the waiter wakes, our instrumentation opens
        # the race window (sleeps 300ms holding no lock). During that
        # window, a newcomer thread enters and sees @size_waiters > 0.
        2.times { release_holders << :go }

        # Give a tiny bit of time for the waiter to receive the signal
        # and enter the injected sleep.
        sleep 0.05

        newcomer_latency = nil
        newcomer = Thread.new do
          t0 = Mongo::Utils.monotonic_time
          conn = pool.check_out
          newcomer_latency = Mongo::Utils.monotonic_time - t0
          pool.check_in(conn)
        end

        holders.each(&:join)
        newcomer.join
        waiter_release << :go
        waiter.join

        # The newcomer entered @size_cv.wait with capacity already available
        # (the original waiter woke and took only one of the two freed slots).
        # Without the baton-pass fix, no further signal arrives and the
        # newcomer waits until its deadline (wait_queue_timeout) before the
        # wait returns due to timeout, at which point the predicate is
        # re-evaluated and passes. Observable symptom: newcomer latency
        # equals wait_queue_timeout.
        #
        # With the fix, the successful waiter signals the next thread in
        # the queue on exit, and the newcomer wakes promptly.
        expect(newcomer_latency).to be < 1.0,
                                    'newcomer waited ' \
                                    "#{(newcomer_latency * 1000).round}ms for a connection " \
                                    'that was already available (lost-wakeup regression)'
      ensure
        size_cv.singleton_class.send(:remove_method, :wait) if size_cv.singleton_class.method_defined?(:wait)
      end
    end
  end
end

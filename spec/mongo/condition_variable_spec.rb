# frozen_string_literal: true
# encoding: utf-8

require 'lite_spec_helper'

describe Mongo::ConditionVariable do
  let(:lock) { Mutex.new }
  let(:condition_variable) do
    described_class.new(lock)
  end

  it 'waits until signaled' do
    result = nil

    consumer = Thread.new do
      lock.synchronize do
        result = condition_variable.wait(3)
      end
    end

    # Context switch to start the thread
    sleep 0.1

    start_time = Mongo::Utils.monotonic_time
    lock.synchronize do
      condition_variable.signal
    end
    consumer.join

    (Mongo::Utils.monotonic_time - start_time).should < 1

    result.should be true
  end

  it 'waits until broadcast' do
    result = nil

    consumer = Thread.new do
      lock.synchronize do
        result = condition_variable.wait(3)
      end
    end

    # Context switch to start the thread
    sleep 0.1

    start_time = Mongo::Utils.monotonic_time
    lock.synchronize do
      condition_variable.broadcast
    end
    consumer.join

    (Mongo::Utils.monotonic_time - start_time).should < 1

    result.should be true
  end

  it 'times out' do
    result = nil

    consumer = Thread.new do
      lock.synchronize do
        result = condition_variable.wait(2)
      end
    end

    # Context switch to start the thread
    sleep 0.1

    start_time = Mongo::Utils.monotonic_time
    consumer.join

    (Mongo::Utils.monotonic_time - start_time).should > 1

    result.should be false
  end

  context "when acquiring the lock and waiting" do

    it "releases the lock while waiting" do

      lock_acquired = false
      Timeout::timeout(1) do
        thread = Thread.new do
          until lock_acquired
            sleep 0.1
          end
          lock.synchronize do
            condition_variable.signal
          end
        end
        lock.synchronize do
          lock_acquired = true
          condition_variable.wait(10)
        end
      end
    end

    it "addresses the waiting threads in order" do
      t1_waiting = false
      t2_waiting = false
      t3_waiting = false
      order = []
      threads = []
      threads << Thread.new do
        lock.synchronize do
          t1_waiting = true
          unless condition_variable.wait(10)
            fail "condition variable timed out"
          end
          order << 1
        end
      end
      threads << Thread.new do
        until t1_waiting
          sleep 0.1
        end
        lock.synchronize do
          t2_waiting = true
          unless condition_variable.wait(10)
            fail "condition variable timed out"
          end
          order << 2
        end
      end
      threads << Thread.new do
        until t2_waiting
          sleep 0.1
        end
        lock.synchronize do
          t3_waiting = true
          unless condition_variable.wait(10)
            fail "condition variable timed out"
          end
          order << 3
        end
      end

      until t3_waiting
        sleep 0.1
      end

      3.times do
        lock.synchronize do
          condition_variable.signal
        end
      end
      threads.map(&:join)

      expect(order).to eq([ 1, 2, 3 ])
    end
  end
end

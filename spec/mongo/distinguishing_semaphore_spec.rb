# frozen_string_literal: true
# rubocop:todo all

require 'lite_spec_helper'

describe Mongo::DistinguishingSemaphore do
  let(:semaphore) do
    described_class.new
  end

  it 'waits until signaled' do
    result = nil

    consumer = Thread.new do
      result = semaphore.wait(3)
    end

    # Context switch to start the thread
    sleep 0.1

    start_time = Mongo::Utils.monotonic_time
    semaphore.signal
    consumer.join

    (Mongo::Utils.monotonic_time - start_time).should < 1

    result.should be true
  end

  it 'waits until broadcast' do
    result = nil

    consumer = Thread.new do
      result = semaphore.wait(3)
    end

    # Context switch to start the thread
    sleep 0.1

    start_time = Mongo::Utils.monotonic_time
    semaphore.broadcast
    consumer.join

    (Mongo::Utils.monotonic_time - start_time).should < 1

    result.should be true
  end

  it 'times out' do
    result = nil

    consumer = Thread.new do
      result = semaphore.wait(2)
    end

    # Context switch to start the thread
    sleep 0.1

    start_time = Mongo::Utils.monotonic_time
    consumer.join

    (Mongo::Utils.monotonic_time - start_time).should > 1

    result.should be false
  end
end

# frozen_string_literal: true
# rubocop:todo all

require 'lite_spec_helper'

describe Mongo::Semaphore do
  let(:semaphore) do
    described_class.new
  end

  it 'waits until signaled' do
    consumer = Thread.new do
      semaphore.wait(3)
    end

    # Context switch to start the thread
    sleep 0.1

    start_time = Mongo::Utils.monotonic_time
    semaphore.signal
    consumer.join

    (Mongo::Utils.monotonic_time - start_time).should < 1
  end

  it 'waits until broadcast' do
    consumer = Thread.new do
      semaphore.wait(3)
    end

    # Context switch to start the thread
    sleep 0.1

    start_time = Mongo::Utils.monotonic_time
    semaphore.broadcast
    consumer.join

    (Mongo::Utils.monotonic_time - start_time).should < 1
  end

  it 'times out' do
    consumer = Thread.new do
      semaphore.wait(2)
    end

    # Context switch to start the thread
    sleep 0.1

    start_time = Mongo::Utils.monotonic_time
    consumer.join

    (Mongo::Utils.monotonic_time - start_time).should > 1
  end
end

# frozen_string_literal: true
# encoding: utf-8

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

    expect(Mongo::Utils.monotonic_time - start_time).to be < 1

    expect(result).to be true
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

    expect(Mongo::Utils.monotonic_time - start_time).to be < 1

    expect(result).to be true
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

    expect(Mongo::Utils.monotonic_time - start_time).to be > 1

    expect(result).to be false
  end
end

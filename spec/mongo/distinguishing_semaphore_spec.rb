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

    start_time = Time.now
    semaphore.signal
    consumer.join

    (Time.now - start_time).should < 1

    result.should be true
  end

  it 'waits until broadcast' do
    result = nil

    consumer = Thread.new do
      result = semaphore.wait(3)
    end

    # Context switch to start the thread
    sleep 0.1

    start_time = Time.now
    semaphore.broadcast
    consumer.join

    (Time.now - start_time).should < 1

    result.should be true
  end

  it 'times out' do
    result = nil

    consumer = Thread.new do
      result = semaphore.wait(2)
    end

    # Context switch to start the thread
    sleep 0.1

    start_time = Time.now
    consumer.join

    (Time.now - start_time).should > 1

    result.should be false
  end
end

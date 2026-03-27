# frozen_string_literal: true

require 'lite_spec_helper'

describe Mongo::Retryable::TokenBucket do
  describe '#initialize' do
    it 'starts full with default capacity' do
      bucket = described_class.new
      expect(bucket.capacity).to eq(1000)
      expect(bucket.tokens).to eq(1000)
    end

    it 'starts full with custom capacity' do
      bucket = described_class.new(capacity: 10)
      expect(bucket.capacity).to eq(10)
      expect(bucket.tokens).to eq(10)
    end
  end

  describe '#consume' do
    it 'succeeds when tokens are available' do
      bucket = described_class.new(capacity: 5)
      expect(bucket.consume(1)).to be true
      expect(bucket.tokens).to eq(4)
    end

    it 'fails when tokens are insufficient' do
      bucket = described_class.new(capacity: 1)
      expect(bucket.consume(1)).to be true
      expect(bucket.consume(1)).to be false
      expect(bucket.tokens).to eq(0)
    end

    it 'consumes the specified number of tokens' do
      bucket = described_class.new(capacity: 10)
      expect(bucket.consume(3)).to be true
      expect(bucket.tokens).to eq(7)
    end

    it 'defaults to consuming 1 token' do
      bucket = described_class.new(capacity: 5)
      bucket.consume
      expect(bucket.tokens).to eq(4)
    end
  end

  describe '#deposit' do
    it 'adds tokens to the bucket' do
      bucket = described_class.new(capacity: 10)
      bucket.consume(5)
      bucket.deposit(3)
      expect(bucket.tokens).to eq(8)
    end

    it 'caps at capacity' do
      bucket = described_class.new(capacity: 10)
      bucket.deposit(5)
      expect(bucket.tokens).to eq(10)
    end

    it 'caps at capacity after partial consumption' do
      bucket = described_class.new(capacity: 10)
      bucket.consume(2)
      bucket.deposit(5)
      expect(bucket.tokens).to eq(10)
    end
  end

  describe 'thread safety' do
    # Use capacity 2000, start at 1000 tokens.
    # With 500 consumes and 500 deposits, floor/ceiling cannot be hit:
    #   min possible = 1000 - 500 = 500 > 0 (all consumes succeed)
    #   max possible = 1000 + 500 = 1500 < 2000 (all deposits effective)
    # So the net change is guaranteed to be 0, making the assertion reliable.
    let(:bucket) do
      b = described_class.new(capacity: 2000)
      b.consume(1000)
      b
    end

    def run_concurrent_operations(bucket)
      threads = []
      10.times { threads << Thread.new { 50.times { bucket.consume(1) } } }
      5.times { threads << Thread.new { 100.times { bucket.deposit(1) } } }
      threads.each(&:join)
    end

    it 'handles concurrent consume and deposit' do
      run_concurrent_operations(bucket)
      expect(bucket.tokens).to eq(1000)
    end
  end
end

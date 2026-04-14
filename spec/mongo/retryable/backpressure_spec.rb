# frozen_string_literal: true

require 'lite_spec_helper'

describe Mongo::Retryable::Backpressure do
  describe 'constants' do
    it 'defines BASE_BACKOFF as 0.1 seconds' do
      expect(described_class::BASE_BACKOFF).to eq(0.1)
    end

    it 'defines MAX_BACKOFF as 10 seconds' do
      expect(described_class::MAX_BACKOFF).to eq(10)
    end

    it 'defines DEFAULT_MAX_RETRIES as 2' do
      expect(described_class::DEFAULT_MAX_RETRIES).to eq(2)
    end
  end

  describe '.backoff_delay' do
    it 'returns 0 when jitter is 0' do
      expect(described_class.backoff_delay(1, jitter: 0)).to eq(0)
      expect(described_class.backoff_delay(5, jitter: 0)).to eq(0)
    end

    it 'returns exact exponential values when jitter is 1' do
      expect(described_class.backoff_delay(1, jitter: 1)).to eq(0.1)
      expect(described_class.backoff_delay(2, jitter: 1)).to eq(0.2)
      expect(described_class.backoff_delay(3, jitter: 1)).to eq(0.4)
      expect(described_class.backoff_delay(4, jitter: 1)).to eq(0.8)
      expect(described_class.backoff_delay(5, jitter: 1)).to eq(1.6)
    end

    it 'caps at MAX_BACKOFF for large attempt numbers' do
      expect(described_class.backoff_delay(100, jitter: 1)).to eq(10)
    end

    it 'returns a value between 0 and the expected max with default jitter' do
      100.times do
        delay = described_class.backoff_delay(1)
        expect(delay).to be >= 0
        expect(delay).to be < 0.1
      end
    end
  end
end

# frozen_string_literal: true

require 'lite_spec_helper'

describe Mongo::Retryable::RetryPolicy do
  describe '#initialize' do
    it 'defaults max_retries to DEFAULT_MAX_RETRIES' do
      policy = described_class.new
      default = Mongo::Retryable::Backpressure::DEFAULT_MAX_RETRIES
      expect(policy.should_retry_overload?(default, 0.1)).to be true
      expect(policy.should_retry_overload?(default + 1, 0.1)).to be false
    end

    it 'accepts a custom max_retries' do
      policy = described_class.new(max_retries: 5)
      expect(policy.should_retry_overload?(5, 0.1)).to be true
      expect(policy.should_retry_overload?(6, 0.1)).to be false
    end

    it 'accepts max_retries of 0' do
      policy = described_class.new(max_retries: 0)
      expect(policy.should_retry_overload?(1, 0.1)).to be false
    end
  end

  describe '#should_retry_overload?' do
    let(:policy) { described_class.new(max_retries: 2) }

    it 'allows retries up to max_retries' do
      expect(policy.should_retry_overload?(1, 0.1)).to be true
      expect(policy.should_retry_overload?(2, 0.1)).to be true
    end

    it 'denies retries beyond max_retries' do
      expect(policy.should_retry_overload?(3, 0.1)).to be false
    end

    context 'with CSOT context' do
      it 'denies retry when delay would exceed deadline' do
        expired = instance_double(Mongo::Operation::Context,
                                  csot?: true,
                                  deadline: Mongo::Utils.monotonic_time - 1)
        expect(policy.should_retry_overload?(1, 0.1, context: expired)).to be false
      end

      it 'allows retry when delay fits within deadline' do
        future = instance_double(Mongo::Operation::Context,
                                 csot?: true,
                                 deadline: Mongo::Utils.monotonic_time + 100)
        expect(policy.should_retry_overload?(1, 0.1, context: future)).to be true
      end

      it 'allows retry when deadline is zero (unlimited)' do
        unlimited = instance_double(Mongo::Operation::Context,
                                    csot?: true,
                                    deadline: 0)
        expect(policy.should_retry_overload?(1, 0.1, context: unlimited)).to be true
      end
    end
  end

  describe '#backoff_delay' do
    it 'delegates to Backpressure.backoff_delay' do
      policy = described_class.new
      result = policy.backoff_delay(1, jitter: 1.0)
      expected = Mongo::Retryable::Backpressure.backoff_delay(1, jitter: 1.0)
      expect(result).to eq(expected)
    end
  end
end

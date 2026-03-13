# frozen_string_literal: true

require 'lite_spec_helper'

describe Mongo::Retryable::RetryPolicy do
  describe '#initialize' do
    context 'without adaptive retries' do
      let(:policy) { described_class.new }

      it 'does not create a token bucket' do
        expect(policy.token_bucket).to be_nil
      end
    end

    context 'with adaptive retries' do
      let(:policy) { described_class.new(adaptive_retries: true) }

      it 'creates a token bucket' do
        expect(policy.token_bucket).to be_a(Mongo::Retryable::TokenBucket)
      end
    end
  end

  describe '#backoff_delay' do
    let(:policy) { described_class.new }

    it 'delegates to Backpressure.backoff_delay' do
      expect(policy.backoff_delay(1, jitter: 1)).to eq(0.1)
      expect(policy.backoff_delay(3, jitter: 1)).to eq(0.4)
    end
  end

  describe '#should_retry_overload?' do
    context 'without adaptive retries' do
      let(:policy) { described_class.new }

      it 'allows retries up to MAX_RETRIES' do
        expect(policy.should_retry_overload?(1, 0.1)).to be true
        expect(policy.should_retry_overload?(5, 0.1)).to be true
      end

      it 'denies retries beyond MAX_RETRIES' do
        expect(policy.should_retry_overload?(6, 0.1)).to be false
      end
    end

    context 'with adaptive retries' do
      let(:policy) { described_class.new(adaptive_retries: true) }

      it 'consumes a token on each retry' do
        bucket = policy.token_bucket
        # drain all but 2 tokens
        (1000 - 2).times { bucket.consume(1) }

        expect(policy.should_retry_overload?(1, 0.1)).to be true
        expect(policy.should_retry_overload?(2, 0.1)).to be true
        expect(policy.should_retry_overload?(3, 0.1)).to be false
      end
    end

    context 'with CSOT context' do
      let(:policy) { described_class.new }

      it 'denies retry when delay would exceed deadline' do
        context = double('context', csot?: true, deadline: Mongo::Utils.monotonic_time + 0.01)
        expect(policy.should_retry_overload?(1, 100, context: context)).to be false
      end

      it 'allows retry when delay fits within deadline' do
        context = double('context', csot?: true, deadline: Mongo::Utils.monotonic_time + 100)
        expect(policy.should_retry_overload?(1, 0.1, context: context)).to be true
      end

      it 'allows retry when deadline is zero (unlimited)' do
        context = double('context', csot?: true, deadline: 0)
        expect(policy.should_retry_overload?(1, 100, context: context)).to be true
      end
    end
  end

  describe '#record_success' do
    context 'with adaptive retries' do
      let(:policy) { described_class.new(adaptive_retries: true) }

      it 'deposits RETRY_TOKEN_RETURN_RATE on first attempt success' do
        bucket = policy.token_bucket
        bucket.consume(10)
        initial = bucket.tokens
        policy.record_success(is_retry: false)
        expect(bucket.tokens).to eq(initial + 0.1)
      end

      it 'deposits RETRY_TOKEN_RETURN_RATE + 1 on retry success' do
        bucket = policy.token_bucket
        bucket.consume(10)
        initial = bucket.tokens
        policy.record_success(is_retry: true)
        expect(bucket.tokens).to eq(initial + 1.1)
      end
    end

    context 'without adaptive retries' do
      let(:policy) { described_class.new }

      it 'does nothing' do
        expect { policy.record_success(is_retry: false) }.not_to raise_error
      end
    end
  end

  describe '#record_non_overload_retry_failure' do
    context 'with adaptive retries' do
      let(:policy) { described_class.new(adaptive_retries: true) }

      it 'deposits 1 token' do
        bucket = policy.token_bucket
        bucket.consume(10)
        initial = bucket.tokens
        policy.record_non_overload_retry_failure
        expect(bucket.tokens).to eq(initial + 1)
      end
    end

    context 'without adaptive retries' do
      let(:policy) { described_class.new }

      it 'does nothing' do
        expect { policy.record_non_overload_retry_failure }.not_to raise_error
      end
    end
  end
end

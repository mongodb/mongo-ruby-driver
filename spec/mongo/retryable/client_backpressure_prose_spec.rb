# frozen_string_literal: true

require 'lite_spec_helper'

# Prose tests from the client-backpressure specification:
# specifications/source/client-backpressure/tests/README.md
describe 'Client Backpressure Prose Tests' do
  # Shared helpers ----------------------------------------------------------

  def make_overload_error(message = 'overloaded')
    Mongo::Error::OperationFailure.new(
      message, nil,
      code: 462,
      code_name: 'IngressRequestRateLimitExceeded',
      labels: %w[RetryableWriteError SystemOverloadedError RetryableError]
    )
  end

  let(:cluster) { double('cluster') }
  let(:server) { double('server') }
  let(:server_selector) { double('server_selector') }

  let(:session) do
    double('session', retry_reads?: true, in_transaction?: false)
  end

  let(:context) do
    double('context', remaining_timeout_sec: nil, csot?: false, deadline: nil).tap do |ctx|
      allow(ctx).to receive(:check_timeout!)
    end
  end

  # -------------------------------------------------------------------------
  # Test 1: Operation Retry Uses Exponential Backoff
  # -------------------------------------------------------------------------
  describe 'Test 1: operation retry uses exponential backoff' do
    let(:retry_policy) { Mongo::Retryable::RetryPolicy.new }

    let(:client) do
      double('client').tap do |c|
        allow(c).to receive(:retry_policy).and_return(retry_policy)
        allow(c).to receive(:cluster).and_return(cluster)
      end
    end

    let(:retryable) do
      double('retryable', client: client, cluster: cluster).tap do |r|
        allow(r).to receive(:select_server).and_return(server)
      end
    end

    let(:worker) { Mongo::Retryable::ReadWorker.new(retryable) }

    it 'with jitter=0 backoff is near-zero; with jitter~1 backoff >= 2.1s' do
      sleep_args = []
      allow(worker).to receive(:sleep) { |d| sleep_args << d }

      max_retries = Mongo::Retryable::Backpressure::MAX_RETRIES

      # Run with jitter=0 (no backoff).
      allow(retry_policy).to receive(:backoff_delay) { |attempt|
        Mongo::Retryable::Backpressure.backoff_delay(attempt, jitter: 0.0)
      }

      sleep_args.clear
      call_count = 0
      expect do
        worker.read_with_retry(session, server_selector, context) do |_s, _r|
          call_count += 1
          raise make_overload_error
        end
      end.to raise_error(Mongo::Error::OperationFailure)

      no_backoff_total = sleep_args.sum

      # Run with jitter~1 (maximum backoff).
      allow(retry_policy).to receive(:backoff_delay) { |attempt|
        Mongo::Retryable::Backpressure.backoff_delay(attempt, jitter: 1.0)
      }

      sleep_args.clear
      call_count = 0
      expect do
        worker.read_with_retry(session, server_selector, context) do |_s, _r|
          call_count += 1
          raise make_overload_error
        end
      end.to raise_error(Mongo::Error::OperationFailure)

      with_backoff_total = sleep_args.sum

      # The spec says the difference should be >= 2.1 seconds.
      # With jitter=1 the total is 0.1+0.2+0.4+0.8+1.6 = 3.1s;
      # with jitter=0 the total is 0.0s.
      expect(with_backoff_total - no_backoff_total).to be >= 2.1
    end
  end

  # -------------------------------------------------------------------------
  # Test 2: Token Bucket Capacity is Enforced
  # -------------------------------------------------------------------------
  describe 'Test 2: token bucket capacity is enforced' do
    it 'starts at DEFAULT_RETRY_TOKEN_CAPACITY and never exceeds it' do
      policy = Mongo::Retryable::RetryPolicy.new(adaptive_retries: true)
      bucket = policy.token_bucket
      capacity = Mongo::Retryable::Backpressure::DEFAULT_RETRY_TOKEN_CAPACITY

      # Assert initial capacity.
      expect(bucket.tokens).to eq(capacity)
      expect(bucket.capacity).to eq(capacity)

      # Simulate a successful (non-retry) command - deposits
      # RETRY_TOKEN_RETURN_RATE (0.1) tokens.
      policy.record_success(is_retry: false)

      # Tokens must not exceed capacity.
      expect(bucket.tokens).to be <= capacity
      expect(bucket.tokens).to eq(capacity)
    end
  end

  # -------------------------------------------------------------------------
  # Test 3: Overload Errors are Retried MAX_RETRIES Times
  # -------------------------------------------------------------------------
  describe 'Test 3: overload errors are retried MAX_RETRIES times' do
    let(:retry_policy) { Mongo::Retryable::RetryPolicy.new }

    let(:client) do
      double('client').tap do |c|
        allow(c).to receive(:retry_policy).and_return(retry_policy)
        allow(c).to receive(:cluster).and_return(cluster)
      end
    end

    let(:retryable) do
      double('retryable', client: client, cluster: cluster).tap do |r|
        allow(r).to receive(:select_server).and_return(server)
      end
    end

    let(:worker) { Mongo::Retryable::ReadWorker.new(retryable) }

    before { allow(worker).to receive(:sleep) }

    it 'attempts the command exactly MAX_RETRIES + 1 times' do
      max_retries = Mongo::Retryable::Backpressure::MAX_RETRIES
      call_count = 0

      expect do
        worker.read_with_retry(session, server_selector, context) do |_s, _r|
          call_count += 1
          raise make_overload_error
        end
      end.to raise_error(Mongo::Error::OperationFailure) { |e|
        expect(e.label?('RetryableError')).to be true
        expect(e.label?('SystemOverloadedError')).to be true
      }

      expect(call_count).to eq(max_retries + 1)
    end
  end

  # -------------------------------------------------------------------------
  # Test 4: Adaptive Retries are Limited by Token Bucket Tokens
  # -------------------------------------------------------------------------
  describe 'Test 4: adaptive retries are limited by token bucket tokens' do
    let(:retry_policy) { Mongo::Retryable::RetryPolicy.new(adaptive_retries: true) }

    let(:client) do
      double('client').tap do |c|
        allow(c).to receive(:retry_policy).and_return(retry_policy)
        allow(c).to receive(:cluster).and_return(cluster)
      end
    end

    let(:retryable) do
      double('retryable', client: client, cluster: cluster).tap do |r|
        allow(r).to receive(:select_server).and_return(server)
      end
    end

    let(:worker) { Mongo::Retryable::ReadWorker.new(retryable) }

    before { allow(worker).to receive(:sleep) }

    it 'retries only as many times as there are tokens (2 tokens -> 3 total attempts)' do
      bucket = retry_policy.token_bucket

      # Drain the bucket down to exactly 2 tokens.
      bucket.consume(bucket.capacity)
      bucket.deposit(2)
      expect(bucket.tokens).to eq(2)

      call_count = 0

      expect do
        worker.read_with_retry(session, server_selector, context) do |_s, _r|
          call_count += 1
          raise make_overload_error
        end
      end.to raise_error(Mongo::Error::OperationFailure) { |e|
        expect(e.label?('RetryableError')).to be true
        expect(e.label?('SystemOverloadedError')).to be true
      }

      # 1 initial attempt + 2 retries (one token consumed per retry).
      expect(call_count).to eq(3)
    end
  end
end

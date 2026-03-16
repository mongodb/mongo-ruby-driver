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

  let(:cluster) { instance_double(Mongo::Cluster) }
  let(:server) { instance_double(Mongo::Server) }
  let(:server_selector) { instance_double(Mongo::ServerSelector::Primary) }

  let(:session) do
    instance_double(Mongo::Session, retry_reads?: true, in_transaction?: false)
  end

  let(:context) do
    instance_double(
      Mongo::Operation::Context,
      remaining_timeout_sec: nil, csot?: false, deadline: nil
    ).tap { |ctx| allow(ctx).to receive(:check_timeout!) }
  end

  # Shared client/retryable/worker setup for read-worker prose tests.
  shared_context 'with read worker' do
    let(:retry_policy) { Mongo::Retryable::RetryPolicy.new }

    let(:client) do
      instance_double(Mongo::Client).tap do |c|
        allow(c).to receive(:retry_policy).and_return(retry_policy)
        allow(c).to receive(:cluster).and_return(cluster)
      end
    end

    let(:retryable) do
      instance_double(Mongo::Collection, client: client, cluster: cluster).tap do |r|
        allow(r).to receive(:select_server).and_return(server)
      end
    end

    let(:worker) { Mongo::Retryable::ReadWorker.new(retryable) }

    before { allow(worker).to receive(:sleep) }
  end

  # -------------------------------------------------------------------------
  # Test 1: Operation Retry Uses Exponential Backoff
  # -------------------------------------------------------------------------
  describe 'Test 1: operation retry uses exponential backoff' do
    include_context 'with read worker'

    let(:sleep_args) { [] }

    before do
      allow(worker).to receive(:sleep) { |d| sleep_args << d }
    end

    def total_sleep_with_jitter(jitter_value)
      allow(retry_policy).to receive(:backoff_delay) { |attempt|
        Mongo::Retryable::Backpressure.backoff_delay(attempt, jitter: jitter_value)
      }
      sleep_args.clear
      begin
        worker.read_with_retry(session, server_selector, context) { |_s, _r| raise make_overload_error }
      rescue Mongo::Error::OperationFailure
        # expected
      end
      sleep_args.sum
    end

    it 'with jitter=0 backoff is near-zero; with jitter~1 backoff >= 2.1s' do
      no_backoff = total_sleep_with_jitter(0.0)
      with_backoff = total_sleep_with_jitter(1.0)
      expect(with_backoff - no_backoff).to be >= 2.1
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

      expect(bucket.tokens).to eq(capacity)
      policy.record_success(is_retry: false)
      expect(bucket.tokens).to be <= capacity
    end
  end

  # -------------------------------------------------------------------------
  # Test 3: Overload Errors are Retried MAX_RETRIES Times
  # -------------------------------------------------------------------------
  describe 'Test 3: overload errors are retried MAX_RETRIES times' do
    include_context 'with read worker'

    it 'attempts the command exactly MAX_RETRIES + 1 times' do
      call_count = 0
      expect do
        worker.read_with_retry(session, server_selector, context) do |_s, _r|
          call_count += 1
          raise make_overload_error
        end
      end.to raise_error(Mongo::Error::OperationFailure)

      expect(call_count).to eq(Mongo::Retryable::Backpressure::MAX_RETRIES + 1)
    end
  end

  # -------------------------------------------------------------------------
  # Test 4: Adaptive Retries are Limited by Token Bucket Tokens
  # -------------------------------------------------------------------------
  describe 'Test 4: adaptive retries are limited by token bucket tokens' do
    include_context 'with read worker'

    let(:retry_policy) { Mongo::Retryable::RetryPolicy.new(adaptive_retries: true) }

    before do
      bucket = retry_policy.token_bucket
      bucket.consume(bucket.capacity)
      bucket.deposit(2)
    end

    it 'retries only as many times as there are tokens (2 tokens -> 3 total attempts)' do
      call_count = 0
      expect do
        worker.read_with_retry(session, server_selector, context) do |_s, _r|
          call_count += 1
          raise make_overload_error
        end
      end.to raise_error(Mongo::Error::OperationFailure)

      expect(call_count).to eq(3)
    end
  end
end

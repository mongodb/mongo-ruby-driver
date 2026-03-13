# frozen_string_literal: true

require 'lite_spec_helper'

describe Mongo::Retryable::ReadWorker do
  subject(:worker) { described_class.new(retryable) }

  let(:retry_policy) { Mongo::Retryable::RetryPolicy.new }

  let(:client) do
    double('client').tap do |c|
      allow(c).to receive(:retry_policy).and_return(retry_policy)
      allow(c).to receive(:cluster).and_return(cluster)
    end
  end

  let(:cluster) { double('cluster') }

  let(:session) do
    double('session', retry_reads?: true, in_transaction?: false)
  end

  let(:server_selector) { double('server_selector') }
  let(:server) { double('server') }

  let(:context) do
    double('context', remaining_timeout_sec: nil, csot?: false, deadline: nil).tap do |ctx|
      allow(ctx).to receive(:check_timeout!)
    end
  end

  let(:retryable) do
    double('retryable', client: client, cluster: cluster).tap do |r|
      allow(r).to receive(:select_server).and_return(server)
    end
  end

  before do
    allow(worker).to receive(:sleep)
  end

  def make_overload_error(message = 'overloaded')
    Mongo::Error::OperationFailure.new(
      message, nil,
      code: 462,
      code_name: 'IngressRequestRateLimitExceeded',
      labels: %w[RetryableWriteError SystemOverloadedError RetryableError]
    )
  end

  def make_retryable_error(message = 'not master')
    Mongo::Error::OperationFailure.new(
      message, nil,
      code: 10_107,
      code_name: 'NotMaster',
      labels: %w[RetryableWriteError]
    )
  end

  describe '#read_with_retry with overload errors' do
    context 'when an overload error is raised and then succeeds' do
      it 'retries with backoff and returns the result' do
        call_count = 0
        result = worker.read_with_retry(session, server_selector, context) do |_server, _is_retry|
          call_count += 1
          raise make_overload_error if call_count <= 3

          :success
        end

        expect(result).to eq(:success)
        expect(call_count).to eq(4)
        expect(worker).to have_received(:sleep).at_least(2).times
      end
    end

    context 'when overload errors exceed MAX_RETRIES' do
      it 'raises after MAX_RETRIES' do
        max = Mongo::Retryable::Backpressure::MAX_RETRIES
        call_count = 0

        expect do
          worker.read_with_retry(session, server_selector, context) do |_server, _is_retry|
            call_count += 1
            raise make_overload_error
          end
        end.to raise_error(Mongo::Error::OperationFailure, /overloaded/)

        # 1 initial + up to MAX_RETRIES retries
        expect(call_count).to be <= max + 1
      end
    end

    context 'when a non-overload retryable error is raised' do
      it 'retries only once (standard modern retry)' do
        call_count = 0

        result = worker.read_with_retry(session, server_selector, context) do |_server, _is_retry|
          call_count += 1
          raise make_retryable_error if call_count == 1

          :success
        end

        expect(result).to eq(:success)
        expect(call_count).to eq(2)
      end
    end

    context 'when adaptive retries are enabled and bucket is drained' do
      let(:retry_policy) { Mongo::Retryable::RetryPolicy.new(adaptive_retries: true) }

      it 'raises when the token bucket is empty' do
        bucket = retry_policy.token_bucket
        bucket.tokens.to_i.times { bucket.consume(1) }

        expect do
          worker.read_with_retry(session, server_selector, context) do |_server, _is_retry|
            raise make_overload_error
          end
        end.to raise_error(Mongo::Error::OperationFailure, /overloaded/)
      end
    end

    context 'record_success is called on success' do
      it 'calls record_success(is_retry: false) on first-attempt success' do
        expect(retry_policy).to receive(:record_success).with(is_retry: false)

        worker.read_with_retry(session, server_selector, context) do |_server|
          :ok
        end
      end

      it 'calls record_success(is_retry: true) after a retry succeeds' do
        call_count = 0

        # record_success should be called with is_retry: true after the
        # overload retry loop succeeds
        expect(retry_policy).to receive(:record_success).with(is_retry: true)

        worker.read_with_retry(session, server_selector, context) do |_server, _is_retry|
          call_count += 1
          raise make_overload_error if call_count == 1

          :ok
        end
      end
    end
  end
end

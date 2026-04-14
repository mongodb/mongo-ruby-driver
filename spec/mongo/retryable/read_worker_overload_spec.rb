# frozen_string_literal: true

require 'lite_spec_helper'

describe Mongo::Retryable::ReadWorker do
  let(:worker) { described_class.new(retryable) }

  let(:retry_policy) { Mongo::Retryable::RetryPolicy.new }

  let(:client) do
    instance_double(Mongo::Client).tap do |c|
      allow(c).to receive(:retry_policy).and_return(retry_policy)
      allow(c).to receive(:cluster).and_return(cluster)
    end
  end

  let(:cluster) { instance_double(Mongo::Cluster) }

  let(:session) do
    instance_double(Mongo::Session, retry_reads?: true, in_transaction?: false)
  end

  let(:server_selector) { instance_double(Mongo::ServerSelector::Primary) }
  let(:server) { instance_double(Mongo::Server) }

  let(:context) do
    instance_double(Mongo::Operation::Context, remaining_timeout_sec: nil, csot?: false, deadline: nil).tap do |ctx|
      allow(ctx).to receive(:check_timeout!)
    end
  end

  let(:retryable) do
    instance_double(Mongo::Collection, client: client, cluster: cluster).tap do |r|
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
        expect(worker).to receive(:sleep).at_least(:once)
        call_count = 0
        result = worker.read_with_retry(session, server_selector, context) do |_server, _is_retry|
          call_count += 1
          raise make_overload_error if call_count <= 2

          :success
        end

        expect(result).to eq(:success)
        expect(call_count).to eq(3)
      end
    end

    context 'when overload errors exceed DEFAULT_MAX_RETRIES' do
      it 'raises after DEFAULT_MAX_RETRIES' do
        max = Mongo::Retryable::Backpressure::DEFAULT_MAX_RETRIES
        call_count = 0

        expect do
          worker.read_with_retry(session, server_selector, context) do |_server, _is_retry|
            call_count += 1
            raise make_overload_error
          end
        end.to raise_error(Mongo::Error::OperationFailure, /overloaded/)

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
  end
end

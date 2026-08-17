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

    # backoff = jitter * min(MAX_BACKOFF, BASE_BACKOFF * 2**attempt)
    it 'returns BASE_BACKOFF * 2**attempt when jitter is 1' do
      expect(described_class.backoff_delay(1, jitter: 1)).to eq(0.2)
      expect(described_class.backoff_delay(2, jitter: 1)).to eq(0.4)
      expect(described_class.backoff_delay(3, jitter: 1)).to eq(0.8)
      expect(described_class.backoff_delay(4, jitter: 1)).to eq(1.6)
      expect(described_class.backoff_delay(5, jitter: 1)).to eq(3.2)
    end

    it 'caps at MAX_BACKOFF for large attempt numbers' do
      expect(described_class.backoff_delay(100, jitter: 1)).to eq(10)
    end

    it 'returns a value between 0 and the expected max with default jitter' do
      100.times do
        delay = described_class.backoff_delay(1)
        expect(delay).to be >= 0
        expect(delay).to be < 0.2
      end
    end

    it 'uses the default base backoff when no error is given' do
      expect(described_class.backoff_delay(1, jitter: 1, err: nil)).to eq(0.2)
    end
  end

  describe '.backoff_delay with a server-supplied baseBackoffMS' do
    let(:reply_document) do
      {
        'code' => 462,
        'codeName' => 'IngressRequestRateLimitExceeded',
        'errorLabels' => %w[SystemOverloadedError RetryableError],
      }.merge(extra_fields)
    end

    let(:extra_fields) do
      {}
    end

    # Built by hand rather than by Protocol::Reply::deserialize, so the fields
    # need to be set directly.
    let(:reply) do
      Mongo::Protocol::Reply.new.tap do |r|
        r.instance_variable_set(:@documents, [ reply_document ])
        r.instance_variable_set(:@flags, [])
      end
    end

    let(:error) do
      Mongo::Error::OperationFailure.new(
        'overloaded',
        Mongo::Operation::Result.new(reply, Mongo::Server::Description.new(''))
      )
    end

    context 'when baseBackoffMS is positive' do
      let(:extra_fields) do
        { 'baseBackoffMS' => 50 }
      end

      it 'uses it in place of BASE_BACKOFF' do
        # These are the delays prose test 5 measures: 0.05 * 2 and 0.05 * 4.
        expect(described_class.backoff_delay(1, jitter: 1, err: error)).to eq(0.1)
        expect(described_class.backoff_delay(2, jitter: 1, err: error)).to eq(0.2)
      end

      it 'still applies jitter and the MAX_BACKOFF cap' do
        expect(described_class.backoff_delay(1, jitter: 0, err: error)).to eq(0)
        expect(described_class.backoff_delay(100, jitter: 1, err: error)).to eq(10)
      end
    end

    context 'when baseBackoffMS is absent' do
      it 'uses BASE_BACKOFF' do
        expect(described_class.backoff_delay(1, jitter: 1, err: error)).to eq(0.2)
      end
    end

    context 'when baseBackoffMS is zero' do
      let(:extra_fields) do
        { 'baseBackoffMS' => 0 }
      end

      # The spec requires the override only when the value is positive.
      it 'uses BASE_BACKOFF' do
        expect(described_class.backoff_delay(1, jitter: 1, err: error)).to eq(0.2)
      end
    end

    context 'when baseBackoffMS is negative' do
      let(:extra_fields) do
        { 'baseBackoffMS' => -50 }
      end

      it 'uses BASE_BACKOFF' do
        expect(described_class.backoff_delay(1, jitter: 1, err: error)).to eq(0.2)
      end
    end

    context 'when the error carries no result' do
      # The connection pool labels network errors raised during connection
      # establishment with SystemOverloadedError and RetryableError, so an
      # error without a result can reach the overload retry loops.
      let(:error) do
        Mongo::Error::SocketError.new('connection reset').tap do |err|
          err.add_label('SystemOverloadedError')
          err.add_label('RetryableError')
        end
      end

      it 'uses BASE_BACKOFF' do
        expect(described_class.backoff_delay(1, jitter: 1, err: error)).to eq(0.2)
      end
    end
  end
end

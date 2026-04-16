# frozen_string_literal: true

require 'spec_helper'

describe 'backpressure examples' do
  RETRYABLE_ERROR_LABEL = 'RetryableError'
  SYSTEM_OVERLOADED_ERROR = 'SystemOverloadedError'
  BASE_BACKOFF_MS = 100
  MAX_BACKOFF_MS = 10_000

  def system_overloaded_error?(error)
    error.respond_to?(:label?) && error.label?(SYSTEM_OVERLOADED_ERROR)
  end

  def calculate_exponential_backoff(attempt)
    rand * [ MAX_BACKOFF_MS, BASE_BACKOFF_MS * (2**(attempt - 1)) ].min
  end

  def with_retries(max_attempts: 2)
    max_attempts.times do |attempt|
      is_retry = attempt > 0
      if is_retry
        delay = calculate_exponential_backoff(attempt)
        sleep(delay / 1000.0)
      end
      begin
        return yield
      rescue StandardError => e
        is_retryable_overload_error = system_overloaded_error?(e) && e.label?(RETRYABLE_ERROR_LABEL)
        can_retry = is_retryable_overload_error && attempt + 1 < max_attempts
        raise unless can_retry
      end
    end
  end
  describe '#system_overloaded_error?' do
    it 'returns true for an error with the SystemOverloadedError label' do
      error = Mongo::Error.new('overloaded')
      error.add_label(SYSTEM_OVERLOADED_ERROR)
      expect(system_overloaded_error?(error)).to be true
    end

    it 'returns false for an error without the SystemOverloadedError label' do
      error = Mongo::Error.new('other')
      expect(system_overloaded_error?(error)).to be_falsey
    end

    it 'returns false for a plain StandardError' do
      error = StandardError.new('plain')
      expect(system_overloaded_error?(error)).to be_falsey
    end
  end

  describe '#calculate_exponential_backoff' do
    it 'returns a value between 0 and BASE_BACKOFF_MS for the first retry' do
      results = Array.new(100) { calculate_exponential_backoff(1) }
      expect(results).to all(be >= 0)
      expect(results).to all(be <= BASE_BACKOFF_MS)
    end

    it 'caps at MAX_BACKOFF_MS for high attempt numbers' do
      results = Array.new(100) { calculate_exponential_backoff(100) }
      expect(results).to all(be >= 0)
      expect(results).to all(be <= MAX_BACKOFF_MS)
    end
  end

  describe '#with_retries' do
    it 'returns the result of the block on success' do
      result = with_retries { 42 }
      expect(result).to eq(42)
    end

    it 'raises non-retryable errors immediately' do
      attempts = 0
      expect do
        with_retries(max_attempts: 3) do
          attempts += 1
          raise StandardError, 'fatal'
        end
      end.to raise_error(StandardError, 'fatal')
      expect(attempts).to eq(1)
    end

    it 'raises overload errors that lack the RetryableError label' do
      attempts = 0
      expect do
        with_retries(max_attempts: 3) do
          attempts += 1
          error = Mongo::Error.new('overloaded')
          error.add_label(SYSTEM_OVERLOADED_ERROR)
          raise error
        end
      end.to raise_error(Mongo::Error, 'overloaded')
      expect(attempts).to eq(1)
    end

    it 'retries retryable overload errors up to max_attempts' do
      attempts = 0
      expect do
        with_retries(max_attempts: 3) do
          attempts += 1
          error = Mongo::Error.new('overloaded')
          error.add_label(SYSTEM_OVERLOADED_ERROR)
          error.add_label(RETRYABLE_ERROR_LABEL)
          raise error
        end
      end.to raise_error(Mongo::Error, 'overloaded')
      expect(attempts).to eq(3)
    end

    it 'succeeds on retry after a retryable overload error' do
      attempts = 0
      result = with_retries(max_attempts: 3) do
        attempts += 1
        if attempts < 2
          error = Mongo::Error.new('overloaded')
          error.add_label(SYSTEM_OVERLOADED_ERROR)
          error.add_label(RETRYABLE_ERROR_LABEL)
          raise error
        end
        'ok'
      end
      expect(result).to eq('ok')
      expect(attempts).to eq(2)
    end
  end
end

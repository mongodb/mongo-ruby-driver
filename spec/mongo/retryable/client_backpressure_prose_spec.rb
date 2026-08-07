# frozen_string_literal: true

require 'spec_helper'

# Prose tests from the client-backpressure specification:
# specifications/source/client-backpressure/tests/README.md
#
# Test 2 was removed from the specification.
#
# Test 5 (overload errors with baseBackoffMS override base backoff) is not
# implemented here: the driver does not yet read the baseBackoffMS field
# that a 9.0+ server attaches to overload errors.
describe 'Client Backpressure Prose Tests' do
  require_topology :replica_set
  min_server_version '4.4'

  let(:subscriber) { Mrss::EventSubscriber.new }

  let(:client) do
    authorized_client.with(retry_reads: true, retry_writes: true).tap do |client|
      client.subscribe(Mongo::Monitoring::COMMAND, subscriber)
    end
  end

  let(:admin_client) { authorized_client.use(:admin) }

  let(:collection) { client['client-backpressure-prose-test'] }

  # Fail every command in commands with an overload error, that is, one
  # labeled both SystemOverloadedError and RetryableError.
  def set_overload_fail_point(commands, error_code)
    admin_client.command(
      configureFailPoint: 'failCommand',
      mode: 'alwaysOn',
      data: {
        failCommands: commands,
        errorCode: error_code,
        errorLabels: %w[SystemOverloadedError RetryableError]
      }
    )
  end

  def started_events(command_name)
    subscriber.started_events.select { |event| event.command_name == command_name }
  end

  after do
    admin_client.command(configureFailPoint: 'failCommand', mode: 'off')
  rescue Mongo::Error
    # Ignore cleanup failures.
  end

  # -------------------------------------------------------------------------
  # Test 1: Operation Retry Uses Exponential Backoff
  # -------------------------------------------------------------------------
  describe 'Test 1: operation retry uses exponential backoff' do
    # Duration of an insert that fails with an overload error on every
    # attempt, with the random number generator used for jitter pinned to
    # the given value.
    def failing_insert_duration(jitter)
      allow(client.retry_policy).to receive(:rand).and_return(jitter)
      start = Mongo::Utils.monotonic_time
      expect do
        collection.insert_one(a: 1)
      end.to raise_error(Mongo::Error::OperationFailure)
      Mongo::Utils.monotonic_time - start
    end

    it 'waits between retries when jitter is 1 but not when jitter is 0' do
      # Step 3.2: fail every insert with an overload error.
      set_overload_fail_point(%w[insert], 2)

      # Steps 3.1 and 3.3: a jitter of 0 effectively disables backoff.
      no_backoff = failing_insert_duration(0.0)

      # Steps 3.4 and 3.5: a jitter of 1 gives the full backoff.
      with_backoff = failing_insert_duration(1.0)

      # Step 3.6: the sum of the two backoffs is 0.3 seconds. The
      # 0.3-second window accounts for variance between the two runs.
      expect((with_backoff - (no_backoff + 0.3)).abs).to be < 0.3
    end
  end

  # -------------------------------------------------------------------------
  # Test 3: Overload Errors are Retried a Maximum of MAX_RETRIES times
  # -------------------------------------------------------------------------
  describe 'Test 3: overload errors are retried a maximum of MAX_RETRIES times' do
    it 'sends MAX_RETRIES + 1 find commands' do
      # MAX_RETRIES is 2 in the specification.
      expect(client.retry_policy.max_retries).to eq(2)

      # Step 3: fail every find with an overload error.
      set_overload_fail_point(%w[find], 462) # IngressRequestRateLimitExceeded
      subscriber.clear_events!

      # Step 4: perform a find that fails.
      error = begin
        collection.find.first
        nil
      rescue Mongo::Error::OperationFailure => e
        e
      end

      # Step 5: the error carries both labels.
      expect(error).to be_a(Mongo::Error::OperationFailure)
      expect(error.label?('RetryableError')).to be true
      expect(error.label?('SystemOverloadedError')).to be true

      # Step 6: one initial attempt plus MAX_RETRIES retries.
      expect(started_events('find').length).to eq(3)
    end
  end

  # -------------------------------------------------------------------------
  # Test 4: Overload Errors are Retried a Maximum of maxAdaptiveRetries
  # times when configured
  # -------------------------------------------------------------------------
  describe 'Test 4: overload errors are retried a maximum of maxAdaptiveRetries times' do
    # Step 1: a client with maxAdaptiveRetries=1.
    let(:client) do
      authorized_client.with(retry_reads: true, max_adaptive_retries: 1).tap do |client|
        client.subscribe(Mongo::Monitoring::COMMAND, subscriber)
      end
    end

    it 'sends maxAdaptiveRetries + 1 find commands' do
      expect(client.retry_policy.max_retries).to eq(1)

      # Step 3: fail every find with an overload error.
      set_overload_fail_point(%w[find], 462) # IngressRequestRateLimitExceeded
      subscriber.clear_events!

      # Step 4: perform a find that fails.
      error = begin
        collection.find.first
        nil
      rescue Mongo::Error::OperationFailure => e
        e
      end

      # Step 5: the error carries both labels.
      expect(error).to be_a(Mongo::Error::OperationFailure)
      expect(error.label?('RetryableError')).to be true
      expect(error.label?('SystemOverloadedError')).to be true

      # Step 6: one initial attempt plus maxAdaptiveRetries retries.
      expect(started_events('find').length).to eq(2)
    end
  end
end

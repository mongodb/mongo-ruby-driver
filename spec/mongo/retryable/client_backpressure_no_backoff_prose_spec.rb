# frozen_string_literal: true

require 'spec_helper'

# Client Backpressure Prose Tests for backoff behavior.
#
# Spec reference:
#   specifications/source/client-backpressure/tests/README.md
describe 'Client Backpressure backoff prose tests' do
  require_topology :replica_set

  let(:client) do
    authorized_client.with(retry_reads: true)
  end

  let(:admin_client) { client.use(:admin) }

  let(:collection) { client['backoff-prose-test'] }

  let(:subscriber) { Mrss::EventSubscriber.new }

  # The delay of a single backoff at attempt 1 with jitter pinned to 1, per
  # jitter * min(MAX_BACKOFF, BASE_BACKOFF * 2**attempt).
  let(:one_backoff) do
    [
      Mongo::Retryable::Backpressure::MAX_BACKOFF,
      Mongo::Retryable::Backpressure::BASE_BACKOFF * 2,
    ].min
  end

  before do
    # Inflate BASE_BACKOFF so any accidental backoff is clearly visible
    # through timing: without backoff the operation completes in
    # milliseconds. Jitter is pinned so the timing is deterministic.
    stub_const('Mongo::Retryable::Backpressure::BASE_BACKOFF', 0.5)
    allow(client.retry_policy).to receive(:rand).and_return(1.0)
  end

  after do
    admin_client.command(configureFailPoint: 'failCommand', mode: 'off')
  rescue Mongo::Error
    # Ignore cleanup failures.
  end

  # -------------------------------------------------------------------------
  # Test 4: Backoff is applied if and only if the error is an
  # overload error (mixed overload + non-overload in the overload loop)
  # -------------------------------------------------------------------------
  describe 'Test 4: backoff applied only for overload errors in overload retry loop' do
    it 'applies backoff for the overload error but not for subsequent non-overload errors' do
      # Configure first fail point: overload error, fires once.
      admin_client.command(
        configureFailPoint: 'failCommand',
        mode: { times: 1 },
        data: {
          failCommands: %w[find],
          errorCode: 91,
          errorLabels: %w[RetryableError SystemOverloadedError]
        }
      )

      # Via CommandFailedEvent, switch to a non-overload retryable error.
      failpoint_set = false
      client.subscribe(Mongo::Monitoring::COMMAND, subscriber)

      allow(subscriber).to receive(:failed).and_wrap_original do |m, event|
        m.call(event)
        if !failpoint_set && event.command_name == 'find'
          failpoint_set = true
          admin_client.command(
            configureFailPoint: 'failCommand',
            mode: 'alwaysOn',
            data: {
              failCommands: %w[find],
              errorCode: 91,
              errorLabels: %w[RetryableError]
            }
          )
        end
      end

      subscriber.clear_events!

      start_time = Mongo::Utils.monotonic_time
      expect do
        collection.find.first
      end.to raise_error(Mongo::Error::OperationFailure)
      elapsed = Mongo::Utils.monotonic_time - start_time

      # Correct behavior applies exactly one backoff, for the overload error,
      # then retries the non-overload errors immediately. Backing off a second
      # time would add min(MAX_BACKOFF, BASE_BACKOFF * 2**2), i.e. twice as
      # much again, so the upper bound cleanly separates the two.
      expect(elapsed).to be >= one_backoff
      expect(elapsed).to be < one_backoff * 2
    end
  end
end

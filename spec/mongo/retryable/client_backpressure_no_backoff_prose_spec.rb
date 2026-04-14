# frozen_string_literal: true

require 'spec_helper'

# Client Backpressure Prose Tests for backoff behavior.
#
# Spec reference:
#   specifications/source/client-backpressure/tests/README.md
describe 'Client Backpressure backoff prose tests' do
  require_topology :replica_set
  min_server_version '4.4'

  let(:client) do
    authorized_client.with(retry_reads: true)
  end

  let(:admin_client) { client.use(:admin) }

  let(:collection) { client['backoff-prose-test'] }

  let(:subscriber) { Mrss::EventSubscriber.new }

  before do
    # Inflate BASE_BACKOFF so any accidental backoff is clearly visible
    # through timing. Without backoff the operation completes in
    # milliseconds; with backoff it would take at least 5 seconds.
    stub_const('Mongo::Retryable::Backpressure::BASE_BACKOFF', 5.0)
  end

  after do
    admin_client.command(configureFailPoint: 'failCommand', mode: 'off')
  rescue Mongo::Error
    # Ignore cleanup failures.
  end

  # -------------------------------------------------------------------------
  # Test 4: Backoff is not applied for non-overload retryable errors
  # -------------------------------------------------------------------------
  describe 'Test 4: backoff is not applied for non-overload retryable errors' do
    it 'does not apply backoff when retrying a non-overload retryable error' do
      admin_client.command(
        configureFailPoint: 'failCommand',
        mode: { times: 1 },
        data: {
          failCommands: %w[find],
          errorCode: 91,
          errorLabels: %w[RetryableError]
        }
      )

      client.subscribe(Mongo::Monitoring::COMMAND, subscriber)
      subscriber.clear_events!

      start_time = Mongo::Utils.monotonic_time
      collection.find.first
      elapsed = Mongo::Utils.monotonic_time - start_time

      # BASE_BACKOFF is 5s. Without backoff, the operation completes
      # well under 2 seconds even on slow CI.
      expect(elapsed).to be < 2.0

      find_failed = subscriber.failed_events.select { |e| e.command_name == 'find' }
      find_succeeded = subscriber.succeeded_events.select { |e| e.command_name == 'find' }
      expect(find_failed.length).to eq(1)
      expect(find_succeeded.length).to eq(1)
    end
  end

  # -------------------------------------------------------------------------
  # Proposed Test 4: Backoff is applied if and only if the error is an
  # overload error (mixed overload + non-overload in the overload loop)
  # -------------------------------------------------------------------------
  describe 'Test 5: backoff applied only for overload errors in overload retry loop' do
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

      # With BASE_BACKOFF=5s, correct behavior applies one backoff
      # (bounded by BASE_BACKOFF) for the overload error, then retries
      # non-overload errors immediately. The elapsed time should stay
      # under BASE_BACKOFF plus a small margin for network overhead.
      expect(elapsed).to be < Mongo::Retryable::Backpressure::BASE_BACKOFF + 2
    end
  end
end

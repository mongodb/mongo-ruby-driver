# frozen_string_literal: true

require 'spec_helper'

# Retryable Reads Prose Test 1: Test that drivers set the maximum number
# of retries for all retryable read errors when an overload error is
# encountered.
#
# Spec reference:
#   specifications/source/retryable-reads/tests/README.md
#   "1: Test that drivers set the maximum number of retries for all
#    retryable read errors when an overload error is encountered"
describe 'Retryable reads prose test 1: overload retry count' do
  require_topology :replica_set
  min_server_version '6.0'

  let(:client) do
    authorized_client.with(retry_reads: true)
  end

  let(:admin_client) { client.use(:admin) }

  let(:collection) { client['overload-retry-count-reads-prose-test'] }

  let(:subscriber) { Mrss::EventSubscriber.new }

  # MAX_ADAPTIVE_RETRIES in spec terminology; the configured max overload
  # retries for this client (defaults to DEFAULT_MAX_RETRIES).
  let(:max_adaptive_retries) { client.retry_policy.max_retries }

  let(:find_started_events) do
    subscriber.started_events.select { |e| e.command_name == 'find' }
  end

  after do
    admin_client.command(configureFailPoint: 'failCommand', mode: 'off')
  rescue Mongo::Error
    # Ignore cleanup failures.
  end

  it 'makes MAX_ADAPTIVE_RETRIES + 1 total attempts' do
    # Step 2: Configure a fail point for find that fires once with an
    # overload error (code 91, labels RetryableError + SystemOverloadedError).
    admin_client.command(
      configureFailPoint: 'failCommand',
      mode: { times: 1 },
      data: {
        failCommands: %w[find],
        errorCode: 91,
        errorLabels: %w[RetryableError SystemOverloadedError]
      }
    )

    # Step 3: Via CommandFailedEvent, when the first find error fires,
    # configure a second fail point for find with a non-overload retryable
    # error (code 91, label RetryableError only), set to alwaysOn.
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

    # Step 4: Attempt a find. Expect it to fail.
    subscriber.clear_events!
    expect do
      collection.find.first
    end.to raise_error(Mongo::Error::OperationFailure)

    # Step 5: Assert that MAX_ADAPTIVE_RETRIES + 1 total find commands
    # were sent (1 initial attempt + MAX_ADAPTIVE_RETRIES retries).
    expect(find_started_events.length).to eq(max_adaptive_retries + 1)
  end
end

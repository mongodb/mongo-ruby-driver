# frozen_string_literal: true

require 'spec_helper'

# Retryable Writes Prose Test Case 4: Test that drivers set the maximum
# number of retries for all retryable write errors when an overload error
# is encountered.
#
# Spec reference:
#   specifications/source/retryable-writes/tests/README.md
#   "Case 4: Test that drivers set the maximum number of retries for all
#    retryable write errors when an overload error is encountered"
describe 'Retryable writes prose test case 4: overload retry count' do
  require_topology :replica_set
  min_server_version '6.0'

  let(:client) do
    authorized_client.with(retry_writes: true)
  end

  let(:admin_client) { client.use(:admin) }

  let(:collection) { client['overload-retry-count-writes-prose-test'] }

  let(:subscriber) { Mrss::EventSubscriber.new }

  # MAX_ADAPTIVE_RETRIES in spec terminology; the configured max overload
  # retries for this client (defaults to DEFAULT_MAX_RETRIES).
  let(:max_adaptive_retries) { client.retry_policy.max_retries }

  let(:insert_started_events) do
    subscriber.started_events.select { |e| e.command_name == 'insert' }
  end

  after do
    admin_client.command(configureFailPoint: 'failCommand', mode: 'off')
  rescue Mongo::Error
    # Ignore cleanup failures.
  end

  it 'makes MAX_ADAPTIVE_RETRIES + 1 total attempts' do
    # Step 2: Configure a fail point for insert that fires once with an
    # overload error (code 91, labels RetryableError + SystemOverloadedError).
    admin_client.command(
      configureFailPoint: 'failCommand',
      mode: { times: 1 },
      data: {
        failCommands: %w[insert],
        errorCode: 91,
        errorLabels: %w[RetryableError SystemOverloadedError]
      }
    )

    # Step 3: Via CommandFailedEvent, when the first insert error fires,
    # configure a second fail point for insert with a non-overload retryable
    # write error (code 91, labels RetryableError + RetryableWriteError),
    # set to alwaysOn.
    failpoint_set = false
    client.subscribe(Mongo::Monitoring::COMMAND, subscriber)

    allow(subscriber).to receive(:failed).and_wrap_original do |m, event|
      m.call(event)
      if !failpoint_set && event.command_name == 'insert'
        failpoint_set = true
        admin_client.command(
          configureFailPoint: 'failCommand',
          mode: 'alwaysOn',
          data: {
            failCommands: %w[insert],
            errorCode: 91,
            errorLabels: %w[RetryableError RetryableWriteError]
          }
        )
      end
    end

    # Step 4: Attempt an insertOne. Expect it to fail.
    subscriber.clear_events!
    expect do
      collection.insert_one(x: 1)
    end.to raise_error(Mongo::Error::OperationFailure)

    # Step 5: Assert that MAX_ADAPTIVE_RETRIES + 1 total insert commands
    # were sent (1 initial attempt + MAX_ADAPTIVE_RETRIES retries).
    expect(insert_started_events.length).to eq(max_adaptive_retries + 1)
  end
end

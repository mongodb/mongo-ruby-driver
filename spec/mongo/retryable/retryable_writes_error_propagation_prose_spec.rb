# frozen_string_literal: true

require 'spec_helper'

# Retryable Writes Prose Test 6: Test error propagation after
# encountering multiple errors.
#
# Spec reference:
#   specifications/source/retryable-writes/tests/README.md
#   "6. Test error propagation after encountering multiple errors."
describe 'Retryable writes prose test 6: error propagation' do
  require_topology :replica_set
  min_server_version '6.0'

  let(:client) do
    authorized_client.with(retry_writes: true)
  end

  let(:admin_client) { client.use(:admin) }

  let(:collection) { client['error-propagation-prose-test'] }

  after do
    admin_client.command(configureFailPoint: 'failCommand', mode: 'off')
  rescue Mongo::Error
    # Ignore cleanup failures.
  end

  # Case 1: Test that drivers return the correct error when receiving
  # only errors without NoWritesPerformed.
  context 'Case 1: only errors without NoWritesPerformed' do
    it 'returns the most recent error (10107)' do
      # Step 2: Configure a fail point with error code 91 and
      # RetryableError + SystemOverloadedError labels.
      admin_client.command(
        configureFailPoint: 'failCommand',
        mode: { times: 1 },
        data: {
          failCommands: [ 'insert' ],
          errorCode: 91,
          errorLabels: %w[RetryableError SystemOverloadedError]
        }
      )

      # Step 3: Via CommandFailedEvent, configure a fail point with
      # error code 10107 once the 91 error is observed.
      failpoint_set = false
      subscriber = Mrss::EventSubscriber.new
      client.subscribe(Mongo::Monitoring::COMMAND, subscriber)

      allow(subscriber).to receive(:failed).and_wrap_original do |m, event|
        m.call(event)
        if !failpoint_set && event.command_name == 'insert'
          failpoint_set = true
          admin_client.command(
            configureFailPoint: 'failCommand',
            mode: 'alwaysOn',
            data: {
              failCommands: [ 'insert' ],
              errorCode: 10_107,
              errorLabels: %w[RetryableError SystemOverloadedError]
            }
          )
        end
      end

      # Step 4: Attempt an insertOne. Assert error code is 10107.
      error = nil
      begin
        collection.insert_one(x: 1)
      rescue Mongo::Error::OperationFailure => e
        error = e
      end

      expect(error).not_to be_nil
      expect(error.code).to eq(10_107)
    end
  end

  # Case 2: Test that drivers return the correct error when receiving
  # only errors with NoWritesPerformed.
  context 'Case 2: only errors with NoWritesPerformed' do
    it 'returns the first error (91)' do
      # Step 2: Configure a fail point with error code 91 and
      # RetryableError + SystemOverloadedError + NoWritesPerformed labels.
      admin_client.command(
        configureFailPoint: 'failCommand',
        mode: { times: 1 },
        data: {
          failCommands: [ 'insert' ],
          errorCode: 91,
          errorLabels: %w[RetryableError SystemOverloadedError NoWritesPerformed]
        }
      )

      # Step 3: Via CommandFailedEvent, configure a fail point with
      # error code 10107 and NoWritesPerformed once the 91 error is observed.
      failpoint_set = false
      subscriber = Mrss::EventSubscriber.new
      client.subscribe(Mongo::Monitoring::COMMAND, subscriber)

      allow(subscriber).to receive(:failed).and_wrap_original do |m, event|
        m.call(event)
        if !failpoint_set && event.command_name == 'insert'
          failpoint_set = true
          admin_client.command(
            configureFailPoint: 'failCommand',
            mode: 'alwaysOn',
            data: {
              failCommands: [ 'insert' ],
              errorCode: 10_107,
              errorLabels: %w[RetryableError SystemOverloadedError NoWritesPerformed]
            }
          )
        end
      end

      # Step 4: Attempt an insertOne. Assert error code is 91.
      error = nil
      begin
        collection.insert_one(x: 1)
      rescue Mongo::Error::OperationFailure => e
        error = e
      end

      expect(error).not_to be_nil
      expect(error.code).to eq(91)
    end
  end

  # Case 3: Test that drivers return the correct error when receiving
  # some errors with NoWritesPerformed and some without.
  context 'Case 3: mixed errors with and without NoWritesPerformed' do
    it 'returns the error without NoWritesPerformed (91)' do
      # Step 2: Via CommandFailedEvent, configure a fail point with
      # error code 91 and NoWritesPerformed for subsequent retries.
      failpoint_set = false
      subscriber = Mrss::EventSubscriber.new
      client.subscribe(Mongo::Monitoring::COMMAND, subscriber)

      allow(subscriber).to receive(:failed).and_wrap_original do |m, event|
        m.call(event)
        if !failpoint_set && event.command_name == 'insert'
          failpoint_set = true
          admin_client.command(
            configureFailPoint: 'failCommand',
            mode: 'alwaysOn',
            data: {
              failCommands: [ 'insert' ],
              errorCode: 91,
              errorLabels: %w[RetryableError SystemOverloadedError NoWritesPerformed]
            }
          )
        end
      end

      # Step 3: Configure initial fail point with error code 91
      # WITHOUT NoWritesPerformed.
      admin_client.command(
        configureFailPoint: 'failCommand',
        mode: { times: 1 },
        data: {
          failCommands: [ 'insert' ],
          errorCode: 91,
          errorLabels: %w[RetryableError SystemOverloadedError]
        }
      )

      # Step 4: Attempt an insertOne. Assert error code is 91 and
      # error does NOT contain NoWritesPerformed label.
      error = nil
      begin
        collection.insert_one(x: 1)
      rescue Mongo::Error::OperationFailure => e
        error = e
      end

      expect(error).not_to be_nil
      expect(error.code).to eq(91)
      expect(error.label?('NoWritesPerformed')).to be false
    end
  end
end

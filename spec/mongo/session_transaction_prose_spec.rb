# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe Mongo::Session do
  require_topology :replica_set
  min_server_version '4.4'

  describe 'transactions convenient API prose tests' do
    let(:client) { authorized_client }
    let(:admin_client) { authorized_client.use('admin') }
    let(:collection) { client['session-transaction-prose-test'] }

    before do
      collection.delete_many
    end

    after do
      disable_fail_command
    end

    # Prose test from:
    # specifications/source/transactions-convenient-api/tests/README.md
    # ### Retry Backoff is Enforced
    it 'adds measurable delay when jitter is enabled' do
      skip 'failCommand fail point is not available' unless fail_command_available?

      no_backoff_time = with_fixed_jitter(0) do
        with_commit_failures(13) do
          measure_with_transaction_time do |session|
            collection.insert_one({}, session: session)
          end
        end
      end

      with_backoff_time = with_fixed_jitter(1) do
        with_commit_failures(13) do
          measure_with_transaction_time do |session|
            collection.insert_one({}, session: session)
          end
        end
      end

      # Sum of 13 backoffs per spec is approximately 1.8 seconds.
      expect(with_backoff_time).to be_within(0.5).of(no_backoff_time + 1.8)
    end

    private

    def measure_with_transaction_time
      start_time = Mongo::Utils.monotonic_time
      client.start_session do |session|
        session.with_transaction do
          yield(session)
        end
      end
      Mongo::Utils.monotonic_time - start_time
    end

    def with_fixed_jitter(value)
      allow(Random).to receive(:rand).and_return(value)
      yield
    end

    def with_commit_failures(times)
      admin_client.command(
        configureFailPoint: 'failCommand',
        mode: { times: times },
        data: {
          failCommands: ['commitTransaction'],
          errorCode: 251,
        },
      )
      yield
    ensure
      disable_fail_command
    end

    def disable_fail_command
      admin_client.command(configureFailPoint: 'failCommand', mode: 'off')
    rescue Mongo::Error
      # Ignore cleanup failures.
    end

    def fail_command_available?
      admin_client.command(configureFailPoint: 'failCommand', mode: 'off')
      true
    rescue Mongo::Error
      false
    end
  end
end

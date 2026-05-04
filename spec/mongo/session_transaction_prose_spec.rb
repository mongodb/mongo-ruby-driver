# frozen_string_literal: true

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

      no_backoff_sleeps = []
      with_fixed_jitter(0) do
        with_commit_failures(13) do
          client.start_session do |session|
            allow(session).to receive(:sleep) { |d| no_backoff_sleeps << d }
            session.with_transaction { collection.insert_one({}, session: session) }
          end
        end
      end

      with_backoff_sleeps = []
      with_fixed_jitter(1) do
        with_commit_failures(13) do
          client.start_session do |session|
            allow(session).to receive(:sleep) { |d| with_backoff_sleeps << d }
            session.with_transaction { collection.insert_one({}, session: session) }
          end
        end
      end

      # With jitter=0 all requested sleeps are zero; with jitter=1 they sum to
      # approximately 1.8 seconds (sum of 13 exponential backoffs, per spec).
      expect(no_backoff_sleeps.sum).to eq(0)
      expect(with_backoff_sleeps.sum).to be_within(0.05).of(1.8)
    end

    private

    def with_fixed_jitter(value)
      allow(Random).to receive(:rand).and_return(value)
      yield
    end

    def with_commit_failures(times)
      admin_client.command(
        configureFailPoint: 'failCommand',
        mode: { times: times },
        data: {
          failCommands: [ 'commitTransaction' ],
          errorCode: 251,
        }
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

# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2015-2020 MongoDB Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

require 'mongo/retryable/backpressure'
require 'mongo/retryable/token_bucket'
require 'mongo/retryable/retry_policy'
require 'mongo/retryable/read_worker'
require 'mongo/retryable/write_worker'

module Mongo

  # Defines basic behavior around retrying operations.
  #
  # @since 2.1.0
  module Retryable
    extend Forwardable

    # Delegate the public read_with_retry methods to the read_worker
    def_delegators :read_worker,
      :read_with_retry_cursor,
      :read_with_retry,
      :read_with_one_retry

    # Delegate the public write_with_retry methods to the write_worker
    def_delegators :write_worker,
      :write_with_retry,
      :nro_write_with_retry

    # This is a separate method to make it possible for the test suite to
    # assert that server selection is performed during retry attempts.
    #
    # This is a public method so that it can be accessed via the read and
    # write worker delegates, as needed.
    #
    # @api private
    #
    # @return [ Mongo::Server ] A server matching the server preference.
    def select_server(cluster, server_selector, session, failed_server = nil, error: nil, timeout: nil)
      deprioritized = if failed_server && deprioritize_server?(cluster, error)
        [failed_server]
      else
        []
      end
      server_selector.select_server(
        cluster,
        nil,
        session,
        deprioritized: deprioritized,
        timeout: timeout
      )
    end

    private

    # Whether the failed server should be deprioritized during server
    # selection for a retry attempt. For sharded and load-balanced
    # topologies, servers are always deprioritized on any retryable error.
    # For replica sets, servers are only deprioritized when the error
    # carries the SystemOverloadedError label.
    def deprioritize_server?(cluster, error)
      return true if cluster.sharded? || cluster.load_balanced?

      error.respond_to?(:label?) && error.label?('SystemOverloadedError')
    end

    public

    # Returns the read worker for handling retryable reads.
    #
    # @api private
    #
    # @note this is only a public method so that tests can add expectations
    #   based on it.
    def read_worker
      @read_worker ||= ReadWorker.new(self)
    end

    # Returns the write worker for handling retryable writes.
    #
    # @api private
    #
    # @note this is only a public method so that tests can add expectations
    #   based on it.
    def write_worker
      @write_worker ||= WriteWorker.new(self)
    end

    # Wraps an operation with overload retry logic. On overload errors
    # (SystemOverloadedError + RetryableError), retries the block with
    # exponential backoff up to MAX_RETRIES times.
    #
    # The block should include server selection so it is re-done on retry.
    # For cursor operations (getMore), the same server is reused since the
    # cursor is pinned.
    #
    # @param [ Operation::Context | nil ] context The operation context
    #   for CSOT deadline checking.
    # @param [ true | false ] retry_enabled Whether overload retries are
    #   permitted. When false, overload errors are raised immediately
    #   without retrying (used when retryReads/retryWrites is disabled).
    #
    # @return [ Object ] The result of the block.
    #
    # @api private
    def with_overload_retry(context: nil, retry_enabled: true)
      return yield unless retry_enabled

      error_count = 0
      loop do
        begin
          result = yield
          client.retry_policy.record_success(is_retry: error_count > 0)
          return result
        rescue Error::TimeoutError
          raise
        rescue Error::OperationFailure::Family => e
          if e.label?('SystemOverloadedError') && e.label?('RetryableError')
            error_count += 1
            policy = client.retry_policy
            delay = policy.backoff_delay(error_count)
            unless policy.should_retry_overload?(error_count, delay, context: context)
              raise e
            end
            Logger.logger.warn("Overload retry due to: #{e.class.name}: #{e.message}")
            sleep(delay)
          else
            raise e
          end
        end
      end
    end
  end
end

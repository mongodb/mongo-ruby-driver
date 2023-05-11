# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2015-2023 MongoDB Inc.
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

require 'mongo/retryable/base_worker'

module Mongo
  module Retryable

    # Implements the logic around retrying write operations.
    #
    # @api private
    #
    # @since 2.19.0
    class WriteWorker < BaseWorker
      # Implements write retrying functionality by yielding to the passed
      # block one or more times.
      #
      # If the session is provided (hence, the deployment supports sessions),
      # and modern retry writes are enabled on the client, the modern retry
      # logic is invoked. Otherwise the legacy retry logic is invoked.
      #
      # If ending_transaction parameter is true, indicating that a transaction
      # is being committed or aborted, the operation is executed exactly once.
      # Note that, since transactions require sessions, this method will raise
      # ArgumentError if ending_transaction is true and session is nil.
      #
      # @api private
      #
      # @example Execute the write.
      #   write_with_retry do
      #     ...
      #   end
      #
      # @note This only retries operations on not master failures, since it is
      #   the only case we can be sure a partial write did not already occur.
      #
      # @param [ nil | Hash | WriteConcern::Base ] write_concern The write concern.
      # @param [ true | false ] ending_transaction True if the write operation is
      #   abortTransaction or commitTransaction, false otherwise.
      # @param [ Context ] context The context for the operation.
      # @param [ Proc ] block The block to execute.
      #
      # @yieldparam [ Connection ] connection The connection through which the
      #   write should be sent.
      # @yieldparam [ Integer ] txn_num Transaction number (NOT the ACID kind).
      # @yieldparam [ Operation::Context ] context The operation context.
      #
      # @return [ Result ] The result of the operation.
      #
      # @since 2.1.0
      def write_with_retry(write_concern, ending_transaction: false, context:, &block)
        session = context.session

        ensure_valid_state!(ending_transaction, session)

        unless ending_transaction || retry_write_allowed?(session, write_concern)
          return legacy_write_with_retry(nil, context: context, &block)
        end

        # If we are here, session is not nil. A session being nil would have
        # failed retry_write_allowed? check.

        server = select_server(cluster, ServerSelector.primary, session)

        unless ending_transaction || server.retry_writes?
          return legacy_write_with_retry(server, context: context, &block)
        end

        modern_write_with_retry(session, server, context, &block)
      end

      # Retryable writes wrapper for operations not supporting modern retryable
      # writes.
      #
      # If the driver is configured to use modern retryable writes, this method
      # yields to the passed block exactly once, thus not retrying any writes.
      #
      # If the driver is configured to use legacy retryable writes, this method
      # delegates to legacy_write_with_retry which performs write retries using
      # legacy logic.
      #
      # @param [ nil | Hash | WriteConcern::Base ] write_concern The write concern.
      # @param [ Context ] context The context for the operation.
      #
      # @yieldparam [ Connection ] connection The connection through which the
      #   write should be sent.
      # @yieldparam [ nil ] txn_num nil as transaction number.
      # @yieldparam [ Operation::Context ] context The operation context.
      def nro_write_with_retry(write_concern, context:, &block)
        session = context.session
        server = select_server(cluster, ServerSelector.primary, session)
        
        if session&.client.options[:retry_writes]
          begin
            server.with_connection(connection_global_id: context.connection_global_id) do |connection|
              yield connection, nil, context
            end
          rescue *retryable_exceptions, Error::PoolError, Error::OperationFailure => e
            e.add_note('retries disabled')
            raise e
          end
        else
          legacy_write_with_retry(server, context: context, &block)
        end
      end

      # Queries whether the session and write concern support retrying writes.
      #
      # @param [ Mongo::Session ] session The session that the operation is
      #   being run on.
      # @param [ nil | Hash | WriteConcern::Base ] write_concern The write
      #   concern.
      #
      # @return [ true | false ] Whether write retries are allowed or not.
      def retry_write_allowed?(session, write_concern)
        return false unless session&.retry_writes?

        if write_concern.nil?
          true
        else
          WriteConcern.get(write_concern).acknowledged?
        end
      end

      private

      # Makes sure the state of the arguments is consistent and valid.
      #
      # @param [ true | false ] ending_transaction True if the write operation
      #   is abortTransaction or commitTransaction, false otherwise.
      # @param [ nil | Mongo::Session ] session The session that the operation
      #   is being run on (if any).
      def ensure_valid_state!(ending_transaction, session)
        if ending_transaction && !session
          raise ArgumentError, 'Cannot end a transaction without a session'
        end
      end

      # Implements legacy write retrying functionality by yielding to the passed
      # block one or more times.
      #
      # This method is used for operations which are not supported by modern
      # retryable writes, such as delete_many and update_many.
      #
      # @param [ Server ] server The server which should be used for the
      #   operation. If not provided, the current primary will be retrieved from
      #   the cluster.
      # @param [ Context ] context The context for the operation.
      #
      # @yieldparam [ Connection ] connection The connection through which the
      #    write should be sent.
      # @yieldparam [ nil ] txn_num nil as transaction number.
      # @yieldparam [ Operation::Context ] context The operation context.
      #
      # @api private
      def legacy_write_with_retry(server = nil, context:)
        session = context.session

        # This is the pre-session retry logic, and is not subject to
        # current retryable write specifications.
        # In particular it does not retry on SocketError and SocketTimeoutError.
        attempt = 0
        begin
          attempt += 1
          server ||= select_server(cluster, ServerSelector.primary, session)
          server.with_connection(connection_global_id: context.connection_global_id) do |connection|
            # Legacy retries do not use txn_num
            yield connection, nil, context.dup
          end
        rescue Error::OperationFailure => e
          e.add_note('legacy retry')
          e.add_note("attempt #{attempt}")
          server = nil
          if attempt > client.max_write_retries
            raise e
          end
          if e.label?('RetryableWriteError')
            log_retry(e, message: 'Legacy write retry')
            cluster.scan!(false)
            retry
          else
            raise e
          end
        end
      end

      # Implements modern write retrying functionality by yielding to the passed
      # block no more than twice.
      #
      # @param [ Mongo::Session ] session The session that the operation is
      #   being run on.
      # @param [ Server ] server The server which should be used for the
      #   operation.
      # @param [ Operation::Context ] context The context for the operation.
      #
      # @yieldparam [ Connection ] connection The connection through which the
      #    write should be sent.
      # @yieldparam [ Integer ] txn_num Transaction number (NOT the ACID kind).
      # @yieldparam [ Operation::Context ] context The operation context.
      #
      # @return [ Result ] The result of the operation.
      #
      # @api private
      def modern_write_with_retry(session, server, context, &block)
        txn_num = nil
        connection_succeeded = false
        
        server.with_connection(connection_global_id: context.connection_global_id) do |connection|
          connection_succeeded = true

          session.materialize_if_needed
          txn_num = session.in_transaction? ? session.txn_num : session.next_txn_num

          # The context needs to be duplicated here because we will be using
          # it later for the retry as well.
          yield connection, txn_num, context.dup
        end
      rescue *retryable_exceptions, Error::PoolError, Auth::Unauthorized, Error::OperationFailure => e
        e.add_notes('modern retry', 'attempt 1')

        if e.is_a?(Error::OperationFailure)
          ensure_retryable!(e)
        else
          ensure_labeled_retryable!(e, connection_succeeded, session)
        end

        # Context#with creates a new context, which is not necessary here
        # but the API is less prone to misuse this way.
        retry_write(e, txn_num, context: context.with(is_retry: true), &block)
      end

      # Called after a failed write, this will retry the write no more than
      # once.
      #
      # @param [ Exception ] original_error The exception that triggered the
      #   retry.
      # @param [ Number ] txn_num The transaction number.
      # @param [ Operation::Context ] context The context for the operation.
      #
      # @return [ Result ] The result of the operation.
      def retry_write(original_error, txn_num, context:, &block)
        session = context.session

        # We do not request a scan of the cluster here, because error handling
        # for the error which triggered the retry should have updated the
        # server description and/or topology as necessary (specifically,
        # a socket error or a not master error should have marked the respective
        # server unknown). Here we just need to wait for server selection.
        server = select_server(cluster, ServerSelector.primary, session)
        
        unless server.retry_writes?
          # Do not need to add "modern retry" here, it should already be on
          # the first exception.
          original_error.add_note('did not retry because server selected for retry does not support retryable writes')

          # When we want to raise the original error, we must not run the
          # rescue blocks below that add diagnostics because the diagnostics
          # added would either be rendundant (e.g. modern retry note) or wrong
          # (e.g. "attempt 2", we are raising the exception produced in the
          # first attempt and haven't attempted the second time). Use the
          # special marker class to bypass the ordinarily applicable rescues.
          raise Error::RaiseOriginalError
        end
        
        log_retry(original_error, message: 'Write retry')
        server.with_connection(connection_global_id: context.connection_global_id) do |connection|
          yield(connection, txn_num, context)
        end
      rescue *retryable_exceptions, Error::PoolError => e
        fail_on_retryable!(e, original_error)
      rescue Error::OperationFailure => e
        fail_on_operation_failure!(e, original_error)
      rescue Error, Error::AuthError => e
        fail_on_other_error!(e, original_error)
      rescue Error::RaiseOriginalError
        raise original_error
      end

      # Retry writes on MMAPv1 should raise an actionable error; append actionable
      # information to the error message and preserve the backtrace.
      def raise_unsupported_error(e)
        new_error = Error::OperationFailure.new("#{e.class}: #{e} "\
          "This MongoDB deployment does not support retryable writes. Please add "\
          "retryWrites=false to your connection string or use the retry_writes: false Ruby client option")
        new_error.set_backtrace(e.backtrace)
        raise new_error
      end

      # Make sure the exception object is labeled 'RetryableWriteError'. If it
      # isn't, and should not be, re-raise the exception.
      def ensure_labeled_retryable!(e, connection_succeeded, session)
        if !e.label?('RetryableWriteError')
          # If there was an error before the connection was successfully
          # checked out and connected, there was no connection present to use
          # for adding labels. Therefore, we should check if it is retryable,
          # and if it is, add the label and retry it.
          if !connection_succeeded && !session.in_transaction? && e.write_retryable?
            e.add_label('RetryableWriteError')
          else
            raise e
          end
        end
      end

      # Make sure the exception object supports retryable writes. If it does,
      # make sure it has been appropriately labeled. If either condition fails,
      # raise an exception.
      def ensure_retryable!(e)
        if e.unsupported_retryable_write?
          raise_unsupported_error(e)
        elsif !e.label?('RetryableWriteError')
          raise e
        end
      end

      # Raise either e, or original_error, depending on whether e is
      # write_retryable.
      def fail_on_retryable!(e, original_error)
        if e.write_retryable?
          e.add_notes('modern retry', 'attempt 2')
          raise e
        else
          original_error.add_note("later retry failed: #{e.class}: #{e}")
          raise original_error
        end
      end

      # Raise either e, or original_error, depending on whether e is
      # appropriately labeled.
      def fail_on_operation_failure!(e, original_error)
        e.add_note('modern retry')
        if e.label?('RetryableWriteError') && !e.label?('NoWritesPerformed')
          e.add_note('attempt 2')
          raise e
        else
          original_error.add_note("later retry failed: #{e.class}: #{e}")
          raise original_error
        end
      end

      # Raise the original error (after annotating).
      def fail_on_other_error!(e, original_error)
        # Do not need to add "modern retry" here, it should already be on
        # the first exception.
        original_error.add_note("later retry failed: #{e.class}: #{e}")
        raise original_error
      end
    end
  end
end

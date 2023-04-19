# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2019-2020 MongoDB Inc.
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

module Mongo
  module Operation

    # Shared behavior of response handling for operations.
    #
    # @api private
    module ResponseHandling

      private

      # @param [ Mongo::Operation::Result ] result The operation result.
      # @param [ Mongo::Server::Connection ] connection The connection on which
      #   the operation is performed.
      # @param [ Mongo::Operation::Context ] context The operation context.
      def validate_result(result, connection, context)
        unpin_maybe(context.session, connection) do
          add_error_labels(connection, context) do
            add_server_diagnostics(connection) do
              result.validate!
            end
          end
        end
      end

      # Adds error labels to exceptions raised in the yielded to block,
      # which should perform MongoDB operations and raise Mongo::Errors on
      # failure. This method handles network errors (Error::SocketError)
      # and server-side errors (Error::OperationFailure); it does not
      # handle server selection errors (Error::NoServerAvailable), for which
      # labels are added in the server selection code.
      #
      # @param [ Mongo::Server::Connection ] connection The connection on which
      #   the operation is performed.
      # @param [ Mongo::Operation::Context ] context The operation context.
      def add_error_labels(connection, context)
        begin
          yield
        rescue Mongo::Error::SocketError => e
          if context.in_transaction? && !context.committing_transaction?
            e.add_label('TransientTransactionError')
          end
          if context.committing_transaction?
            e.add_label('UnknownTransactionCommitResult')
          end

          maybe_add_retryable_write_error_label!(e, connection, context)

          raise e
        rescue Mongo::Error::SocketTimeoutError => e
          maybe_add_retryable_write_error_label!(e, connection, context)
          raise e
        rescue Mongo::Error::OperationFailure => e
          if context.committing_transaction?
            if e.write_retryable? || e.wtimeout? || (e.write_concern_error? &&
                !Session::UNLABELED_WRITE_CONCERN_CODES.include?(e.write_concern_error_code)
            ) || e.max_time_ms_expired?
              e.add_label('UnknownTransactionCommitResult')
            end
          end

          maybe_add_retryable_write_error_label!(e, connection, context)

          raise e
        end
      end

      # Unpins the session and/or the connection if  the yielded to block
      # raises errors that are required to unpin the session and the connection.
      #
      # @note This method takes the session as an argument because this module
      #   is included in BulkWrite which does not store the session in the
      #   receiver (despite Specifiable doing so).
      #
      # @param [ Session | nil ] session Session to consider.
      # @param [ Connection | nil ] connection Connection to unpin.
      def unpin_maybe(session, connection)
        yield
      rescue Mongo::Error => e
        if session
          session.unpin_maybe(e, connection)
        end
        raise
      end

      # Yields to the block and, if the block raises an exception, adds a note
      # to the exception with the address of the specified server.
      #
      # This method is intended to add server address information to exceptions
      # raised during execution of operations on servers.
      def add_server_diagnostics(connection)
        yield
      rescue Error::SocketError, Error::SocketTimeoutError
        # Diagnostics should have already been added by the connection code,
        # do not add them again.
        raise
      rescue Error, Error::AuthError => e
        e.add_note("on #{connection.address.seed}")
        e.generation = connection.generation
        e.service_id = connection.service_id
        raise e
      end

      private

      # A method that will add the RetryableWriteError label to an error if
      # any of the following conditions are true:
      #
      # The error meets the criteria for a retryable error (i.e. has one
      #   of the retryable error codes or error messages)
      #
      # AND the server does not support adding the RetryableWriteError label OR
      #   the error is a network error (i.e. the driver must add the label)
      #
      # AND the error occured during a commitTransaction or abortTransaction
      #   OR the error occured during a write outside of a transaction on a
      #   client that has retry writes enabled.
      #
      # If these conditions are met, the original error will be mutated.
      # If they're not met, the error will not be changed.
      #
      # @param [ Mongo::Error ] error The error to which to add the label.
      # @param [ Mongo::Server::Connection ] connection The connection on which
      #   the operation is performed.
      # @param [ Mongo::Operation::Context ] context The operation context.
      #
      # @note The client argument is optional because some operations, such as
      #   end_session, do not pass the client as an argument to the execute
      #   method.
      def maybe_add_retryable_write_error_label!(error, connection, context)
        # An operation is retryable if it meets one of the following criteria:
        # - It is a commitTransaction or abortTransaction
        # - It does not occur during a transaction and the client has enabled
        #   modern or legacy writes
        #
        # Note: any write operation within a transaction (excepting commit and
        # abort is NOT a retryable operation)
        retryable_operation = context.committing_transaction? ||
          context.aborting_transaction? ||
          !context.in_transaction? && context.any_retry_writes?

        # An operation should add the RetryableWriteError label if one of the
        # following conditions is met:
        # - The server does not support adding the RetryableWriteError label
        # - The error is a network error
        should_add_error_label =
          !connection.description.features.retryable_write_error_label_enabled? ||
          error.write_concern_error_label?('RetryableWriteError') ||
          error.is_a?(Mongo::Error::SocketError) ||
          error.is_a?(Mongo::Error::SocketTimeoutError)

        if retryable_operation && should_add_error_label && error.write_retryable?
          error.add_label('RetryableWriteError')
        end
      end
    end
  end
end

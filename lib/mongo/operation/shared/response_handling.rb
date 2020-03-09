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

      def validate_result(result, server)
        unpin_maybe(session) do
          add_error_labels do
            add_server_diagnostics(server) do
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
      def add_error_labels
        begin
          yield
        rescue Mongo::Error::SocketError => e
          if session && session.in_transaction? && !session.committing_transaction?
            e.add_label('TransientTransactionError')
          end
          if session && session.committing_transaction?
            e.add_label('UnknownTransactionCommitResult')
          end
          raise e
        rescue Mongo::Error::OperationFailure => e
          if session && session.committing_transaction?
            if e.write_retryable? || e.wtimeout? || (e.write_concern_error? &&
                !Session::UNLABELED_WRITE_CONCERN_CODES.include?(e.write_concern_error_code)
            ) || e.max_time_ms_expired?
              e.add_label('UnknownTransactionCommitResult')
            end
          end
          raise e
        end
      end

      # Unpins the session if the session is pinned and the yielded to block
      # raises errors that are required to unpin the session.
      #
      # @note This method takes the session as an argument because this module
      #   is included in BulkWrite which does not store the session in the
      #   receiver (despite Specifiable doing so).
      #
      # @param [ Session | nil ] Session to consider.
      def unpin_maybe(session)
        yield
      rescue Mongo::Error => e
        if session
          session.unpin_maybe(e)
        end
        raise
      end

      # Yields to the block and, if the block raises an exception, adds a note
      # to the exception with the address of the specified server.
      #
      # This method is intended to add server address information to exceptions
      # raised during execution of operations on servers.
      def add_server_diagnostics(server)
        yield
      rescue Mongo::Error, Mongo::Error::AuthError => e
        e.add_note("on #{server.address.seed}")
        raise e
      end
    end
  end
end

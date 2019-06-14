# Copyright (C) 2015-2019 MongoDB, Inc.
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
  class Error

    # Raised when an operation fails for some reason.
    #
    # @since 2.0.0
    class OperationFailure < Error
      extend Forwardable
      include SdamErrorDetection

      # Error codes and code names that should result in a failing write
      # being retried.
      #
      # @since 2.6.0
      # @api private
      WRITE_RETRY_ERRORS = [
        {:code_name => 'InterruptedAtShutdown', :code => 11600},
        {:code_name => 'InterruptedDueToStepDown', :code => 11602},
        {:code_name => 'NotMaster', :code => 10107},
        {:code_name => 'NotMasterNoSlaveOk', :code => 13435},
        {:code_name => 'NotMasterOrSecondary', :code => 13436},
        {:code_name => 'PrimarySteppedDown', :code => 189},
        {:code_name => 'ShutdownInProgress', :code => 91},
        {:code_name => 'HostNotFound', :code => 7},
        {:code_name => 'HostUnreachable', :code => 6},
        {:code_name => 'NetworkTimeout', :code => 89},
        {:code_name => 'SocketException', :code => 9001},
      ].freeze

      # These are magic error messages that could indicate a master change.
      #
      # @since 2.4.2
      # @api private
      WRITE_RETRY_MESSAGES = [
        'not master',
        'node is recovering',
      ].freeze

      # These are magic error messages that could indicate a cluster
      # reconfiguration behind a mongos.
      #
      # @since 2.1.1
      # @api private
      RETRY_MESSAGES = WRITE_RETRY_MESSAGES + [
        'transport error',
        'socket exception',
        "can't connect",
        'connect failed',
        'error querying',
        'could not get last error',
        'connection attempt failed',
        'interrupted at shutdown',
        'unknown replica set',
        'dbclient error communicating with server'
      ].freeze

      def_delegators :@result, :operation_time

      # @return [ Integer ] The error code parsed from the document.
      #
      # @since 2.6.0
      attr_reader :code

      # @return [ String ] The error code name parsed from the document.
      #
      # @since 2.6.0
      attr_reader :code_name

      # Whether the error is a retryable error according to the legacy read retry
      # logic.
      #
      # @return [ true, false ]
      #
      # @since 2.1.1
      # @deprecated
      def retryable?
        RETRY_MESSAGES.any?{ |m| message.include?(m) }
      end

      # Whether the error is a retryable error according to the modern retryable
      # reads and retryable writes specifications.
      #
      # This method is also used by the legacy retryable write logic to determine
      # whether an error is a retryable one.
      #
      # @return [ true, false ]
      #
      # @since 2.4.2
      def write_retryable?
        WRITE_RETRY_MESSAGES.any? { |m| message.include?(m) } ||
        write_retryable_code?
      end

      def write_retryable_code?
        if code
          WRITE_RETRY_ERRORS.any? { |e| e[:code] == code }
        else
          # return false rather than nil
          false
        end
      end
      private :write_retryable_code?

      # Error codes and code names that should result in a failing getMore
      # command on a change stream NOT being resumed.
      #
      # @since 2.6.0
      # @api private
      CHANGE_STREAM_NOT_RESUME_ERRORS = [
        {:code_name => 'CappedPositionLost', :code => 136},
        {:code_name => 'CursorKilled', :code => 237},
        {:code_name => 'Interrupted', :code => 11601},
      ].freeze

      # Change stream can be resumed when these error messages are encountered.
      #
      # @since 2.6.0
      # @api private
      CHANGE_STREAM_RESUME_MESSAGES = WRITE_RETRY_MESSAGES

      # Can the change stream on which this error occurred be resumed,
      # provided the operation that triggered this error was a getMore?
      #
      # @example Is the error resumable for the change stream?
      #   error.change_stream_resumable?
      #
      # @return [ true, false ] Whether the error is resumable.
      #
      # @since 2.6.0
      def change_stream_resumable?
        if @result && @result.is_a?(Mongo::Operation::GetMore::Result)
          !change_stream_not_resumable_label? &&
          (change_stream_resumable_message? ||
          change_stream_resumable_code?)
        else
          false
        end
      end

      def change_stream_resumable_message?
        CHANGE_STREAM_RESUME_MESSAGES.any? { |m| message.include?(m) }
      end
      private :change_stream_resumable_message?

      def change_stream_resumable_code?
        if code
          !CHANGE_STREAM_NOT_RESUME_ERRORS.any? { |e| e[:code] == code }
        else
          true
        end
      end
      private :change_stream_resumable_code?

      def change_stream_not_resumable_label?
        if labels
          labels.include? 'NonResumableChangeStreamError'
        else
          false
        end
      end
      private :change_stream_not_resumable_label?

      # @return [ true | false ] Whether the failure includes a write
      #   concern error. A failure may have a top level error and a write
      #   concern error or either one of the two.
      #
      # @since 2.10.0
      # @api experimental
      def write_concern_error?
        @write_concern_error
      end

      # @return [ Integer | nil ] The error code for the write concern error,
      #   if a write concern error is present and has a code.
      #
      # @since 2.10.0
      # @api experimental
      attr_reader :write_concern_error_code

      # @return [ String | nil ] The code name for the write concern error,
      #   if a write concern error is present and has a code name.
      #
      # @since 2.10.0
      # @api experimental
      attr_reader :write_concern_error_code_name

      # Create the operation failure.
      #
      # @example Create the error object
      #   OperationFailure.new(message, result)
      #
      # @example Create the error object with a code and a code name
      #   OperationFailure.new(message, result, :code => code, :code_name => code_name)
      #
      # @param [ String ] message The error message.
      # @param [ Operation::Result ] result The result object.
      # @param [ Hash ] options Additional parameters.
      #
      # @option options [ Integer ] :code Error code.
      # @option options [ String ] :code_name Error code name.
      # @option options [ true | false ] :write_concern_error Whether the
      #   write concern error is present.
      # @option options [ Integer ] :write_concern_error_code Error code for
      #   write concern error, if any.
      # @option options [ String ] :write_concern_error_code_name Error code
      #   name for write concern error, if any.
      # @option options [ Array<String> ] :labels The set of labels associated
      #   with the error.
      # @option options [ true | false ] :wtimeout Whether the error is a wtimeout.
      #
      # @since 2.5.0, options added in 2.6.0
      def initialize(message = nil, result = nil, options = {})
        @result = result
        @code = options[:code]
        @code_name = options[:code_name]
        @write_concern_error = !!options[:write_concern_error]
        @write_concern_error_code = options[:write_concern_error_code]
        @write_concern_error_code_name = options[:write_concern_error_code_name]
        @labels = options[:labels]
        @wtimeout = !!options[:wtimeout]
        super(message)
      end

      # Whether the error is a write concern timeout.
      #
      # @return [ true | false ] Whether the error is a write concern timeout.
      #
      # @since 2.7.1
      def wtimeout?
        @wtimeout
      end
    end
  end
end

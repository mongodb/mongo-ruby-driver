# Copyright (C) 2015-2017 MongoDB, Inc.
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

      # These are the error codes that indicate that a write is retryable.
      #
      # @since 2.6.0.
      WRITE_RETRY_CODES = [
        11600, # InterruptedAtShutdown
        11602, # InterruptedDueToReplStateChange
        10107, # NotMaster
        13435, # NotMasterNoSlaveOk
        13436, # NotMasterOrSecondary
        189,   # PrimarySteppedDown
        91,    # ShutdownInProgress
        64,    # WriteConcernFailed
        7,     # HostNotFound
        6,     # HostUnreachable
        89,    # NetworkTimeout
        9001   # SocketException
      ].freeze

      # These are magic error messages that could indicate a master change.
      #
      # @since 2.4.2
      WRITE_RETRY_MESSAGES = [
        'no master',
        'not master',
        'could not contact primary',
        'Not primary',
        'node is recovering'
      ].freeze

      # These are magic error messages that could indicate a cluster
      # reconfiguration behind a mongos. We cannot check error codes as they
      # change between versions, for example 15988 which has 2 completely
      # different meanings between 2.4 and 3.0.
      #
      # @since 2.1.1
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

      # @return [ Integer ] code The error code parsed from the document.
      # @since 2.6.0
      attr_reader :code

      # @return [ String ] code_name The error code name parsed from the document.
      # @since 2.6.0
      attr_reader :code_name

      # Can the read operation that caused the error be retried?
      #
      # @example Is the error retryable?
      #   error.retryable?
      #
      # @return [ true, false ] If the error is retryable.
      #
      # @since 2.1.1
      def retryable?
        RETRY_MESSAGES.any?{ |m| message.include?(m) }
      end

      # Can the write operation that caused the error be retried?
      #
      # @example Is the error retryable for writes?
      #   error.write_retryable?
      #
      # @return [ true, false ] If the error is retryable.
      #
      # @since 2.4.2
      def write_retryable?
        return true if WRITE_RETRY_MESSAGES.any? { |m| message.include?(m) }
        return true if WRITE_RETRY_CODES.any? { |c| c == @code }
        return false unless @result

        write_concern_err = @result.send(:first_document)['writeConcernError']
        return false unless write_concern_err

        WRITE_RETRY_MESSAGES.any? { |m| m.include?(write_concern_err['errmsg']) } ||
        WRITE_RETRY_CODES.any? { |c| c == write_concern_err['code'] }
      end

      # Does the error have the given label?
      #
      # @example
      #   error.label?(label)
      #
      # @return [ true, false ] Whether the error has the given label.
      #
      # @since 2.6.0
      def label?(label)
        @labels.include?(label) ||
          (@result.send(:first_document) && @result.send(:first_document)['errorLabels'])
      end

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
      # @param [ Hash ] options Additional parameters
      #
      # @option options [ Integer ] :code Error code
      # @option options [ String ] :code_name Error code name
      #
      # @since 2.5.0, options added in 2.6.0
      def initialize(message = nil, result = nil, options = {})
        @result = result
        @code = options[:code]
        @code_name = options[:code_name]
        @labels = options[:labels] || []
        super(message)
      end

      private

      def add_label(label)
        @labels << label unless label?(label)
      end
    end
  end
end

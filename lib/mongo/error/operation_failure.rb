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
require 'mongo/error/read_write_retryable'

module Mongo
  class Error

    # Raised when an operation fails for some reason.
    #
    # @since 2.0.0
    class OperationFailure < Error
      extend Forwardable
      include SdamErrorDetection
      include ReadWriteRetryable

      def_delegators :@result, :operation_time

      # @!method connection_description
      #
      # @return [ Server::Description ] Server description of the server that
      #   the operation that this exception refers to was performed on.
      #
      # @api private
      def_delegator :@result, :connection_description

      # @return [ Integer ] The error code parsed from the document.
      #
      # @since 2.6.0
      attr_reader :code

      # @return [ String ] The error code name parsed from the document.
      #
      # @since 2.6.0
      attr_reader :code_name

      # @return [ String ] The server-returned error message
      #   parsed from the response.
      #
      # @api experimental
      attr_reader :server_message

      # Error codes and code names that should result in a failing getMore
      # command on a change stream NOT being resumed.
      #
      # @api private
      CHANGE_STREAM_RESUME_ERRORS = [
        {code_name: 'HostUnreachable', code: 6},
        {code_name: 'HostNotFound', code: 7},
        {code_name: 'NetworkTimeout', code: 89},
        {code_name: 'ShutdownInProgress', code: 91},
        {code_name: 'PrimarySteppedDown', code: 189},
        {code_name: 'ExceededTimeLimit', code: 262},
        {code_name: 'SocketException', code: 9001},
        {code_name: 'NotMaster', code: 10107},
        {code_name: 'InterruptedAtShutdown', code: 11600},
        {code_name: 'InterruptedDueToReplStateChange', code: 11602},
        {code_name: 'NotPrimaryNoSecondaryOk', code: 13435},
        {code_name: 'NotMasterOrSecondary', code: 13436},

        {code_name: 'StaleShardVersion', code: 63},
        {code_name: 'FailedToSatisfyReadPreference', code: 133},
        {code_name: 'StaleEpoch', code: 150},
        {code_name: 'RetryChangeStream', code: 234},
        {code_name: 'StaleConfig', code: 13388},
      ].freeze

      # Change stream can be resumed when these error messages are encountered.
      #
      # @since 2.6.0
      # @api private
      CHANGE_STREAM_RESUME_MESSAGES = ReadWriteRetryable::WRITE_RETRY_MESSAGES

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
          # CursorNotFound exceptions are always resumable because the server
          # is not aware of the cursor id, and thus cannot determine if
          # the cursor is a change stream and cannot add the
          # ResumableChangeStreamError label.
          return true if code == 43

          # Connection description is not populated for unacknowledged writes.
          if connection_description.max_wire_version >= 9
            label?('ResumableChangeStreamError')
          else
            change_stream_resumable_code?
          end
        else
          false
        end
      end

      def change_stream_resumable_code?
        CHANGE_STREAM_RESUME_ERRORS.any? { |e| e[:code] == code }
      end
      private :change_stream_resumable_code?

      # @return [ true | false ] Whether the failure includes a write
      #   concern error. A failure may have a top level error and a write
      #   concern error or either one of the two.
      #
      # @since 2.10.0
      def write_concern_error?
        !!@write_concern_error_document
      end

      # Returns the write concern error document as it was reported by the
      # server, if any.
      #
      # @return [ Hash | nil ] Write concern error as reported to the server.
      attr_reader :write_concern_error_document

      # @return [ Integer | nil ] The error code for the write concern error,
      #   if a write concern error is present and has a code.
      #
      # @since 2.10.0
      attr_reader :write_concern_error_code

      # @return [ String | nil ] The code name for the write concern error,
      #   if a write concern error is present and has a code name.
      #
      # @since 2.10.0
      attr_reader :write_concern_error_code_name

      # @return [ String | nil ] The details of the error.
      #   For WriteConcernErrors this is `document['writeConcernError']['errInfo']`.
      #   For WriteErrors this is `document['writeErrors'][0]['errInfo']`.
      #   For all other errors this is nil.
      attr_reader :details

      # @return [ BSON::Document | nil ] The server-returned error document.
      #
      # @api experimental
      attr_reader :document

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
      # @option options [ BSON::Document ] :document The server-returned
      #   error document.
      # @option options [ String ] server_message The server-returned
      #   error message parsed from the response.
      # @option options [ Hash ] :write_concern_error_document The
      #   server-supplied write concern error document, if any.
      # @option options [ Integer ] :write_concern_error_code Error code for
      #   write concern error, if any.
      # @option options [ String ] :write_concern_error_code_name Error code
      #   name for write concern error, if any.
      # @option options [ Array<String> ] :write_concern_error_labels Error
      #   labels for the write concern error, if any.
      # @option options [ Array<String> ] :labels The set of labels associated
      #   with the error.
      # @option options [ true | false ] :wtimeout Whether the error is a wtimeout.
      def initialize(message = nil, result = nil, options = {})
        @details = retrieve_details(options[:document])
        super(append_details(message, @details))

        @result = result
        @code = options[:code]
        @code_name = options[:code_name]
        @write_concern_error_document = options[:write_concern_error_document]
        @write_concern_error_code = options[:write_concern_error_code]
        @write_concern_error_code_name = options[:write_concern_error_code_name]
        @write_concern_error_labels = options[:write_concern_error_labels] || []
        @labels = options[:labels] || []
        @wtimeout = !!options[:wtimeout]
        @document = options[:document]
        @server_message = options[:server_message]
      end

      # Whether the error is a write concern timeout.
      #
      # @return [ true | false ] Whether the error is a write concern timeout.
      #
      # @since 2.7.1
      def wtimeout?
        @wtimeout
      end

      # Whether the error is MaxTimeMSExpired.
      #
      # @return [ true | false ] Whether the error is MaxTimeMSExpired.
      #
      # @since 2.10.0
      def max_time_ms_expired?
        code == 50 # MaxTimeMSExpired
      end

      # Whether the error is caused by an attempted retryable write
      # on a storage engine that does not support retryable writes.
      #
      # @return [ true | false ] Whether the error is caused by an attempted
      # retryable write on a storage engine that does not support retryable writes.
      #
      # @since 2.10.0
      def unsupported_retryable_write?
        # code 20 is IllegalOperation.
        # Note that the document is expected to be a BSON::Document, thus
        # either having string keys or providing indifferent access.
        code == 20 && server_message&.start_with?("Transaction numbers") || false
      end

      private

      # Retrieve the details from a document
      #
      # @return [ Hash | nil ] the details extracted from the document
      def retrieve_details(document)
        return nil unless document
        if wce = document['writeConcernError']
          return wce['errInfo']
        elsif we = document['writeErrors']&.first
          return we['errInfo']
        end
      end

      # Append the details to the message
      #
      # @return [ String ] the message with the details appended to it
      def append_details(message, details)
        return message unless details && message
        message + " -- #{details.to_json}"
      end
    end
  end
end

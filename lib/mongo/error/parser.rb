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

# Sample error - mongo 3.4:
# {
#   "ok" : 0,
#   "errmsg" : "not master",
#   "code" : 10107,
#   "codeName" : "NotMaster"
# }
#
# Sample response with a write concern error - mongo 3.4:
# {
#   "n" : 1,
#   "opTime" : {
#     "ts" : Timestamp(1527728618, 1),
#     "t" : NumberLong(4)
#   },
#   "electionId" : ObjectId("7fffffff0000000000000004"),
#   "writeConcernError" : {
#     "code" : 100,
#     "codeName" : "CannotSatisfyWriteConcern",
#     "errmsg" : "Not enough data-bearing nodes"
#   },
#   "ok" : 1
# }

module Mongo
  class Error

    # Class for parsing the various forms that errors can come in from MongoDB
    # command responses.
    #
    # The errors can be reported by the server in a number of ways:
    # - {ok:0} response indicates failure. In newer servers, code, codeName
    #   and errmsg fields should be set. In older servers some may not be set.
    # - {ok:1} response with a write concern error (writeConcernError top-level
    #   field). This indicates that the node responding successfully executed
    #   the request, but not enough other nodes successfully executed the
    #   request to satisfy the write concern.
    # - {ok:1} response with writeErrors top-level field. This can be obtained
    #   in a bulk write but also in a non-bulk write. In a non-bulk write
    #   there should be exactly one error in the writeErrors list.
    #   The case of multiple errors is handled by BulkWrite::Result.
    # - {ok:1} response with writeConcernErrors top-level field. This can
    #   only be obtained in a bulk write and is handled by BulkWrite::Result,
    #   not by this class.
    #
    # Note that writeErrors do not have codeName fields - they just provide
    # codes and messages. writeConcernErrors may similarly not provide code
    # names.
    #
    # @since 2.0.0
    # @api private
    class Parser
      include SdamErrorDetection

      # @return [ BSON::Document ] The returned document.
      attr_reader :document

      # @return [ String ] The full error message to be used in the
      #   raised exception.
      attr_reader :message

      # @return [ String ] The server-returned error message
      #   parsed from the response.
      attr_reader :server_message

      # @return [ Array<Protocol::Message> ] The message replies.
      attr_reader :replies

      # @return [ Integer ] The error code parsed from the document.
      # @since 2.6.0
      attr_reader :code

      # @return [ String ] The error code name parsed from the document.
      # @since 2.6.0
      attr_reader :code_name

      # @return [ Array<String> ] The set of labels associated with the error.
      # @since 2.7.0
      attr_reader :labels

      # @api private
      attr_reader :wtimeout

      # Create the new parser with the returned document.
      #
      # In legacy mode, the code and codeName fields of the document are not
      # examined because the status (ok: 1) is not part of the document and
      # there is no way to distinguish successful from failed responses using
      # the document itself, and a successful response may legitimately have
      # { code: 123, codeName: 'foo' } as the contents of a user-inserted
      # document. The legacy server versions do not fill out code nor codeName
      # thus not reading them does not lose information.
      #
      # @example Create the new parser.
      #   Parser.new({ 'errmsg' => 'failed' })
      #
      # @param [ BSON::Document ] document The returned document.
      # @param [ Array<Protocol::Message> ] replies The message replies.
      # @param [ Hash ] options The options.
      #
      # @option options [ true | false ] :legacy Whether document and replies
      #   are from a legacy (pre-3.2) response
      #
      # @since 2.0.0
      def initialize(document, replies = nil, options = nil)
        @document = document || {}
        @replies = replies
        @options = if options
          options.dup
        else
          {}
        end.freeze
        parse!
      end

      # @return [ true | false ] Whether the document includes a write
      #   concern error. A failure may have a top level error and a write
      #   concern error or either one of the two.
      #
      # @since 2.10.0
      # @api experimental
      def write_concern_error?
        !!write_concern_error_document
      end

      # Returns the write concern error document as it was reported by the
      # server, if any.
      #
      # @return [ Hash | nil ] Write concern error as reported to the server.
      # @api experimental
      def write_concern_error_document
        document['writeConcernError']
      end

      # @return [ Integer | nil ] The error code for the write concern error,
      #   if a write concern error is present and has a code.
      #
      # @since 2.10.0
      # @api experimental
      def write_concern_error_code
        write_concern_error_document && write_concern_error_document['code']
      end

      # @return [ String | nil ] The code name for the write concern error,
      #   if a write concern error is present and has a code name.
      #
      # @since 2.10.0
      # @api experimental
      def write_concern_error_code_name
        write_concern_error_document && write_concern_error_document['codeName']
      end

      # @return [ Array<String> | nil ] The error labels associated with this
      # write concern error, if there is a write concern error present.
      def write_concern_error_labels
        write_concern_error_document && write_concern_error_document['errorLabels']
      end

      class << self
        def build_message(code: nil, code_name: nil, message: nil)
          if code_name && code
            "[#{code}:#{code_name}]: #{message}"
          elsif code_name
            # This surely should never happen, if there's a code name
            # there ought to also be the code provided.
            # Handle this case for completeness.
            "[#{code_name}]: #{message}"
          elsif code
            "[#{code}]: #{message}"
          else
            message
          end
        end
      end

      private

      def parse!
        if document['ok'] != 1 && document['writeErrors']
          raise ArgumentError, "writeErrors should only be given in successful responses"
        end

        @message = +""
        parse_single(@message, '$err')
        parse_single(@message, 'err')
        parse_single(@message, 'errmsg')
        parse_multiple(@message, 'writeErrors')
        if write_concern_error_document
          parse_single(@message, 'errmsg', write_concern_error_document)
        end
        parse_flag(@message)
        parse_code
        parse_labels
        parse_wtimeout

        @server_message = @message
        @message = self.class.build_message(
          code: code,
          code_name: code_name,
          message: @message,
        )
      end

      def parse_single(message, key, doc = document)
        if error = doc[key]
          append(message, error)
        end
      end

      def parse_multiple(message, key)
        if errors = document[key]
          errors.each do |error|
            parse_single(message, 'errmsg', error)
          end
        end
      end

      def parse_flag(message)
        if replies && replies.first &&
            (replies.first.respond_to?(:cursor_not_found?)) && replies.first.cursor_not_found?
          append(message, CURSOR_NOT_FOUND)
        end
      end

      def append(message, error)
        if message.length > 1
          message.concat(", #{error}")
        else
          message.concat(error)
        end
      end

      def parse_code
        if document['ok'] == 1 || @options[:legacy]
          @code = @code_name = nil
        else
          @code = document['code']
          @code_name = document['codeName']
        end

        # Since there is only room for one code, do not replace
        # codes of the top level response with write concern error codes.
        # In practice this should never be an issue as a write concern
        # can only fail after the operation succeeds on the primary.
        if @code.nil? && @code_name.nil?
          if subdoc = write_concern_error_document
            @code = subdoc['code']
            @code_name = subdoc['codeName']
          end
        end

        if @code.nil? && @code_name.nil?
          # If we have writeErrors, and all of their codes are the same,
          # use that code. Otherwise don't set the code
          if write_errors = document['writeErrors']
            codes = write_errors.map { |e| e['code'] }.compact
            if codes.uniq.length == 1
              @code = codes.first
              # code name may not be returned by the server
              @code_name = write_errors.map { |e| e['codeName'] }.compact.first
            end
          end
        end
      end

      def parse_labels
        @labels = document['errorLabels'] || []
      end

      def parse_wtimeout
        @wtimeout = write_concern_error_document &&
          write_concern_error_document['errInfo'] &&
          write_concern_error_document['errInfo']['wtimeout']
      end
    end
  end
end

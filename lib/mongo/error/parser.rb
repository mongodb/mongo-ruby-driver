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
    # @since 2.0.0
    class Parser
      include SdamErrorDetection

      # @return [ BSON::Document ] document The returned document.
      attr_reader :document

      # @return [ String ] message The error message parsed from the document.
      attr_reader :message

      # @return [ Array<Protocol::Message> ] replies The message replies.
      attr_reader :replies

      # @return [ Integer ] code The error code parsed from the document.
      # @since 2.6.0
      attr_reader :code

      # @return [ String ] code_name The error code name parsed from the document.
      # @since 2.6.0
      attr_reader :code_name

      # @return [ Array<String> ] labels The set of labels associated with the error.
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
      # {code: 123, codeName: 'foo'} as the contents of a user-inserted
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
      def write_concern_error_code_name
        write_concern_error_document && write_concern_error_document['codeName']
      end

      private

      def write_concern_error_document
        document[WRITE_CONCERN_ERROR]
      end

      def parse!
        @message = ""
        parse_single(@message, ERR)
        parse_single(@message, ERROR)
        parse_single(@message, ERRMSG)
        parse_multiple(@message, WRITE_ERRORS)
        if write_concern_error_document
          parse_single(@message, ERRMSG, write_concern_error_document)
        end
        parse_flag(@message)
        parse_code
        parse_labels
        parse_wtimeout
      end

      def parse_single(message, key, doc = document)
        if error = doc[key]
          append(message ,"#{error} (#{doc[CODE]})")
        end
      end

      def parse_multiple(message, key)
        if errors = document[key]
          errors.each do |error|
            parse_single(message, ERRMSG, error)
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
          if write_errors = document[WRITE_ERRORS]
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

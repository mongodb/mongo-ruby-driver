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

    # Class for parsing the various forms that errors can come in from MongoDB
    # command responses.
    #
    # @since 2.0.0
    class Parser

      # @return [ BSON::Document ] document The returned document.
      attr_reader :document

      # @return [ String ] message The error message parsed from the document.
      attr_reader :message

      # @return [ Array<Protocol::Reply> ] replies The message replies.
      attr_reader :replies

      # Create the new parser with the returned document.
      #
      # @example Create the new parser.
      #   Parser.new({ 'errmsg' => 'failed' })
      #
      # @param [ BSON::Document ] document The returned document.
      #
      # @since 2.0.0
      def initialize(document, replies = nil)
        @document = document || {}
        @replies = replies
        parse!
      end

      private

      def parse!
        @message = ""
        parse_single(@message, ERR)
        parse_single(@message, ERROR)
        parse_single(@message, ERRMSG)
        parse_multiple(@message, WRITE_ERRORS)
        parse_single(@message, ERRMSG,
                     document[WRITE_CONCERN_ERROR]) if document[WRITE_CONCERN_ERROR]
        parse_flag(@message)
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
        if replies && replies.first && replies.first.cursor_not_found?
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
    end
  end
end

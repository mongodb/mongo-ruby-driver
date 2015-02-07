# Copyright (C) 2015 MongoDB, Inc.
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

      # Create the new parser with the returned document.
      #
      # @example Create the new parser.
      #   Parser.new({ 'errmsg' => 'failed' })
      #
      # @param [ BSON::Document ] document The returned document.
      #
      # @since 2.0.0
      def initialize(document)
        @document = document
      end

      # Parse the returned document, giving back the extracted error message.
      #
      # @example Parse the document for the errors.
      #   parser.parse
      #
      # @return [ String ] The error message.
      #
      # @since 2.0.0
      def parse
        return @message if @message
        @message = String.new
        parse_single(@message, ERR)
        parse_single(@message, ERROR)
        parse_single(@message, ERRMSG)
        parse_multiple(@message, WRITE_ERRORS)
        parse_multiple(@message, WRITE_CONCERN_ERROR)
        @message
      end

      private

      def parse_single(message, key, doc = document)
        if error = doc[key]
          message.concat("#{error} (#{doc[CODE]})")
        end
      end

      def parse_multiple(message, key)
        if errors = document[key]
          errors.each do |error|
            parse_single(message, ERRMSG, error)
          end
        end
      end
    end
  end
end

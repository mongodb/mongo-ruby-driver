# frozen_string_literal: true

# Copyright (C) 2014-2020 MongoDB Inc.
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
    # Exception that is raised when trying to parse a URI that does not match
    # the specification.
    #
    # @since 2.0.0
    class InvalidURI < Error
      # Instantiate the new exception.
      #
      # The message deliberately contains no part of the connection string.
      # A string that failed to parse cannot be redacted reliably: the parser
      # and the redactor can disagree about where the userinfo ends, so
      # masking the input may still disclose a credential. The details
      # supplied by the parser identify the component at fault by name
      # instead. See the "Disclosure of Sensitive Values" section of the
      # connection string specification.
      #
      # @example Instantiate the exception.
      #   Mongo::Error::InvalidURI.new(uri, details, format)
      #
      # @param [ String | nil ] _uri The URI that failed to parse. Kept in the
      #   signature for compatibility; it is not included in the message.
      # @param [ String ] details Description of the parse failure.
      # @param [ String | nil ] format The expected connection string format.
      #
      # @since 2.0.0
      def initialize(_uri, details, format = nil)
        message = "#{details}\n"
        message += "MongoDB URI must be in the following format: #{format}\n" if format
        message += "Please see the following URL for more information: #{Mongo::URI::HELP}\n"
        super(message)
      end
    end
  end
end

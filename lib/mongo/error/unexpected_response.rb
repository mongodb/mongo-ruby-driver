# frozen_string_literal: true
# rubocop:todo all

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

    # Raised if the response read from the socket does not match the latest query.
    #
    # @since 2.2.6
    class UnexpectedResponse < Error

      # Create the new exception.
      #
      # @example Create the new exception.
      #   Mongo::Error::UnexpectedResponse.new(expected_response_to, response_to)
      #
      # @param [ Integer ] expected_response_to The last request id sent.
      # @param [ Integer ] response_to The actual response_to of the reply.
      #
      # @since 2.2.6
      def initialize(expected_response_to, response_to)
        super("Unexpected response. Got response for request ID #{response_to} " +
              "but expected response for request ID #{expected_response_to}")
      end
    end
  end
end

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

    # Raised when an operation fails for some reason.
    #
    # @since 2.0.0
    class OperationFailure < Error

      # These are magic error messages that could indicate a cluster
      # reconfiguration behind a mongos. We cannot check error codes as they
      # change between versions, for example 15988 which has 2 completely
      # different meanings between 2.4 and 3.0.
      #
      # @since 2.1.1
      RETRY_MESSAGES = [
        'transport error',
        'socket exception',
        "can't connect",
        'no master',
        'not master',
        'connect failed',
        'error querying',
        'could not get last error',
        'connection attempt failed'
      ].freeze

      # Can the operation that caused the error be retried?
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
    end
  end
end

# Copyright (C) 2020 MongoDB Inc.
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
    # Raised if the hint option is specified for an operation but the server
    # selected does not support hints.
    class UnsupportedHint < Error

      # The default error message describing that hints are not supported.
      #
      # @api private
      DEFAULT_MESSAGE = "The MongoDB server handling this request does not " \
        "support the hint option on this command. The hint option is supported " \
        "on update commands on MongoDB server versions 4.2 and later, and " \
        "on findAndModify and delete commands on MongoDB server versions 4.4 " \
        "and later"

      # The default error message describing that hints are not supported on
      #   unacknowledged writes.
      #
      # @api private
      DEFAULT_UNACKNOWLEDGED_MESSAGE = "A hint cannot be specified on an " \
        "operation being performed with an unacknowledged write concern " \
        "({ w: 0}). Remove the hint option or perform this operaiton with " \
        "a write concern of at least { w: 1 }"

      # Create a new UnsupportedHint error.
      #
      # @param [String | nil] message An optional custom error message. If this
      #   argument is nil, a default message will be supplied.
      # @param [Hash] options
      #
      # @option options [Boolean] unacknowledged_write Whether this error is
      #   being raised because a hint was specified on an unacknowledged write.
      #   Defaults to false.
      def initialize(message = nil, options = {})
        unacknowledged_write = options[:unacknowledged_write] || false

        default_message = if unacknowledged_write
          DEFAULT_UNACKNOWLEDGED_MESSAGE
        else
          DEFAULT_MESSAGE
        end

        super(message || default_message)
      end
    end
  end
end

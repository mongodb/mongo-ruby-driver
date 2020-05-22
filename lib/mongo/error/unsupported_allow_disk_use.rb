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
    # Raised if the allow_disk_use option is specified for an operation but the server
    # selected does not support this option.
    class UnsupportedAllowDiskUse < Error

      # The default error message describing that the allow_disk_use option
      # is not supported.
      #
      # @api private
      DEFAULT_MESSAGE = "The MongoDB server handling this request does not " \
        "support the allow_disk_use option on this command. The allow_disk_use " \
        "option is supported on find commands on MongoDB server versions 4.4 " \
        "and later"

      # Create a new UnsupportedAllowDiskUse error.
      #
      # @param [String | nil] message An optional custom error message. If this
      #   argument is nil, a default message will be supplied.
      def initialize(message = nil)
        super(message || DEFAULT_MESSAGE)
      end
    end
  end
end

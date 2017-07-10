# Copyright (C) 2014-2017 MongoDB, Inc.
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

    # Raised if the Grid::FSBucket::Stream object is closed and an operation is attempted.
    #
    # @since 2.1.0
    class ClosedStream < Error

      # Create the new exception.
      #
      # @example Create the new exception.
      #   Mongo::Error::ClosedStream.new
      #
      # @since 2.1.0
      def initialize
        super("The stream is closed and cannot be written to or read from.")
      end
    end
  end
end

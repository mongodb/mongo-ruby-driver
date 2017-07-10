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

    # Raised when an invalid write concern is provided.
    #
    # @since 2.2.0
    class InvalidWriteConcern < Error

      # Instantiate the new exception.
      #
      # @example Instantiate the exception.
      #   Mongo::Error::InvalidWriteConcern.new
      #
      # @since 2.2.0
      def initialize
        super('Invalid write concern options. If w is an Integer, it must be greater than or equal to 0. ' +
              'If w is 0, it cannot be combined with a true value for fsync or j (journal).')
      end
    end
  end
end

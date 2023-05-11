# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2019-present MongoDB Inc.
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

    # Exception raised if an operation is attempted connection that was
    # interrupted due to server monitor timeout.
    class PoolClearedError < PoolError
      include WriteRetryable
      include ChangeStreamResumable

      # Instantiate the new exception.
      #
      # @example Instantiate the exception.
      #   Mongo::Error::PoolClearedError.new(address, pool)
      #
      # @api private
      def initialize(address, pool)
        super(address, pool,
          "Connection to #{address} interrupted due to server monitor timeout " +
            "(for pool 0x#{pool.object_id})")
      end
    end
  end
end

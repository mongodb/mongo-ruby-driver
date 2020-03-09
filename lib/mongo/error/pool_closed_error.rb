# Copyright (C) 2019-2020 MongoDB Inc.
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

    # Exception raised if an operation is attempted on a closed connection pool.
    #
    # @since 2.9.0
    class PoolClosedError < Error

      # @return [ Mongo::Address ] address The address of the server the
      # pool's connections connect to.
      #
      # @since 2.9.0
      attr_reader :address

      # @return [ Mongo::Server::ConnectionPool ] pool The pool that is closed.
      #
      # @since 2.11.0
      # @api experimental
      attr_reader :pool

      # Instantiate the new exception.
      #
      # @example Instantiate the exception.
      #   Mongo::Error::PoolClosedError.new(address, pool)
      #
      # @since 2.9.0
      # @api private
      def initialize(address, pool)
        @address = address
        @pool = pool
        super("Attempted to use a connection pool which has been closed (for #{address} " +
            "with pool 0x#{pool.object_id})")
      end
    end
  end
end

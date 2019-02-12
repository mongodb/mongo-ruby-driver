# Copyright (C) 2014-2019 MongoDB, Inc.
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
    # @since 2.8.0
    class PoolClosed < Error

      # @return [ Mongo::Address ] address The address of the server the pool's connections connect
      #   to.
      #
      # @since 2.8.0
      attr_reader :address

      # @return [ Integer ] pool_size The size of connection pool.
      #
      # @since 2.8.0
      attr_reader :pool_size

      # Instantiate the new exception.
      #
      # @example Instantiate the exception.
      #   Mongo::Error::PoolClosed.new(address, pool_size)
      #
      # @since 2.8.0
      def initialize(address, pool_size)
        @address = address
        @pool_size = pool_size

        super("attempted to check out a connection from closed connection pool with address " +
                  "#{address} and size #{pool_size}")
      end
    end
  end
end

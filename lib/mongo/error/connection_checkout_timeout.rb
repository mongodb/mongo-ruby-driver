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

    # Exception raised if a timeout occurs when attempting to acquire a connection from a pool.
    #
    # @since 2.8.0
    class ConnectionCheckoutTimeout < Error

      # @return [ Mongo::Address ] address The address of the server the pool's connections connect
      #   to.
      #
      # @since 2.8.0
      attr_reader :address

      # Instantiate the new exception.
      #
      # @example Instantiate the exception.
      #   Mongo::Error::ConnectionCheckoutTimeout.new(address, pool_size)
      #
      # @since 2.8.0
      def initialize(address)
        @address = address

        super("Timeout when attempting to check out a connection from pool with address " +
                "#{address}")
      end
    end
  end
end

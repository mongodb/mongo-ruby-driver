# Copyright (C) 2019 MongoDB, Inc.
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

    # Exception raised when trying to check out a connection from a connection
    # pool, the pool is at its max size and no connections become available
    # within the configured wait timeout.
    #
    # @note For backwards compatibility reasons this class derives from
    #   Timeout::Error rather than Mongo::Error.
    #
    # @since 2.9.0
    class ConnectionCheckOutTimeout < ::Timeout::Error

      # @return [ Mongo::Address ] address The address of the server the
      #   pool's connections connect to.
      #
      # @since 2.9.0
      attr_reader :address

      # Instantiate the new exception.
      #
      # @param [ Address ] address
      # @param [ Float ] wait_timeout
      #
      # @since 2.9.0
      # @api private
      def initialize(address, wait_timeout)
        @address = address

        super("Timed out when attempting to check out a connection from pool with address " +
                "#{address} after #{wait_timeout} sec")
      end
    end
  end
end

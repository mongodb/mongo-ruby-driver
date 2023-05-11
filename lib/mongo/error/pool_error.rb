# frozen_string_literal: true
# rubocop:todo all

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

    # Abstract base class for connection pool-related exceptions.
    class PoolError < Error

      # @return [ Mongo::Address ] address The address of the server the
      # pool's connections connect to.
      #
      # @since 2.9.0
      attr_reader :address

      # @return [ Mongo::Server::ConnectionPool ] pool The connection pool.
      #
      # @since 2.11.0
      attr_reader :pool

      # Instantiate the new exception.
      #
      # @api private
      def initialize(address, pool, message)
        @address = address
        @pool = pool
        super(message)
      end
    end
  end
end

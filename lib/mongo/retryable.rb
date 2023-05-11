# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2015-2020 MongoDB Inc.
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

require 'mongo/retryable/read_worker'
require 'mongo/retryable/write_worker'

module Mongo

  # Defines basic behavior around retrying operations.
  #
  # @since 2.1.0
  module Retryable
    extend Forwardable

    # Delegate the public read_with_retry methods to the read_worker
    def_delegators :read_worker,
      :read_with_retry_cursor,
      :read_with_retry,
      :read_with_one_retry

    # Delegate the public write_with_retry methods to the write_worker
    def_delegators :write_worker,
      :write_with_retry,
      :nro_write_with_retry

    # This is a separate method to make it possible for the test suite to
    # assert that server selection is performed during retry attempts.
    #
    # This is a public method so that it can be accessed via the read and
    # write worker delegates, as needed.
    #
    # @api private
    #
    # @return [ Mongo::Server ] A server matching the server preference.
    def select_server(cluster, server_selector, session)
      server_selector.select_server(cluster, nil, session)
    end

    # Returns the read worker for handling retryable reads.
    #
    # @api private
    #
    # @note this is only a public method so that tests can add expectations
    #   based on it.
    def read_worker
      @read_worker ||= ReadWorker.new(self)
    end

    # Returns the write worker for handling retryable writes.
    #
    # @api private
    #
    # @note this is only a public method so that tests can add expectations
    #   based on it.
    def write_worker
      @write_worker ||= WriteWorker.new(self)
    end
  end
end

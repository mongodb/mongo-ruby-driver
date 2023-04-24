# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2021 MongoDB Inc.
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

    # Raised when the driver requires a connection to a particular service
    # but no matching connections exist in the connection pool.
    class NoServiceConnectionAvailable < Error
      # @api private
      def initialize(message, address:, service_id:)
        super(message)

        @address = address
        @service_id = service_id
      end

      # @return [ Mongo::Address ] The address to which a connection was
      #   requested.
      attr_reader :address

      # @return [ nil | Object ] The service id.
      attr_reader :service_id

      # @api private
      def self.generate(address:, service_id:)
        new(
          "The connection pool for #{address} does not have a connection for service #{service_id}",
          address: address,
          service_id: service_id,
        )
      end
    end
  end
end

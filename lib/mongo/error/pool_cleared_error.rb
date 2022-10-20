# frozen_string_literal: true
# encoding: utf-8

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

    # Exception raised if an operation is attempted connection that was
    # interrupted due to server monitor timeout.
    class PoolClearedError < Error
      include WriteRetryable
      include ChangeStreamResumable

      # @return [ Mongo::Address ] address The address of the server the
      # pool's connections connect to.
      attr_reader :address

      # Instantiate the new exception.
      #
      # @example Instantiate the exception.
      #   Mongo::Error::PoolClearedError.new(address)
      #
      # @api private
      def initialize(address)
        @address = address
        super("Connection to #{address} interrupted due to server monitor timeout")
      end
    end
  end
end

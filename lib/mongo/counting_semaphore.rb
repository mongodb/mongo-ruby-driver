# frozen_string_literal: true
# encoding: utf-8

# Copyright (C) 2018-present MongoDB Inc.
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

require 'concurrent'

module Mongo
  # This is a counting semaphore implementation.
  #
  # @api private
  class CountingSemaphore
    extend Forwardable

    def initialize(n = 1)
      @semaphore = Concurrent::Semaphore.new(n)
    end

    def acquire
      @semaphore.acquire
      yield if block_given?
    ensure
      @semaphore.release if block_given?
    end

    def try_acquire(timeout = nil)
      # JRuby does not allow passing a nil or negative timeout to this method.
      res = if timeout && timeout > 0
        @semaphore.try_acquire(1, timeout)
      else
        @semaphore.try_acquire(1)
      end
      return false unless res

      yield if block_given?
    ensure
      @semaphore.release if block_given? && res
    end

    def_delegators :@semaphore, :release, :available_permits
  end
end

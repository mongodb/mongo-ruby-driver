# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2020 MongoDB Inc.
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
  # This is an implementation of a condition variable.
  #
  # @api private
  class ConditionVariable
    extend Forwardable

    def initialize(lock = Mutex.new)
      @lock = lock
      @cv = ::ConditionVariable.new
    end

    # Waits for the condition variable to be signaled up to timeout seconds.
    # If condition variable is not signaled, returns after timeout seconds.
    def wait(timeout = nil)
      raise_unless_locked!
      return false if timeout && timeout < 0
      @cv.wait(@lock, timeout)
    end

    def broadcast
      raise_unless_locked!
      @cv.broadcast
    end

    def signal
      raise_unless_locked!
      @cv.signal
    end

    def_delegators :@lock, :synchronize

    private

    def raise_unless_locked!
      unless @lock.owned?
        raise ArgumentError, "the lock must be owned when calling this method"
      end
    end
  end
end

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
  # This is a semaphore that distinguishes waits ending due to the timeout
  # being reached from waits ending due to the semaphore being signaled.
  #
  # @api private
  class DistinguishingSemaphore
    def initialize
      @lock = Mutex.new
      @cv = ::ConditionVariable.new
      @queue = []
    end

    # Waits for the semaphore to be signaled up to timeout seconds.
    # If semaphore is not signaled, returns after timeout seconds.
    #
    # @return [ true | false ] true if semaphore was signaled, false if
    #   timeout was reached.
    def wait(timeout = nil)
      @lock.synchronize do
        @cv.wait(@lock, timeout)
        (!@queue.empty?).tap do
          @queue.clear
        end
      end
    end

    def broadcast
      @lock.synchronize do
        @queue.push(true)
        @cv.broadcast
      end
    end

    def signal
      @lock.synchronize do
        @queue.push(true)
        @cv.signal
      end
    end
  end
end

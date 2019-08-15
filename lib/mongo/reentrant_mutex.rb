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
  # This is a mutex which can be locked multiple times by the same thread.
  #
  # @api private
  class ReentrantMutex
    def initialize
      @lock = Mutex.new
      @depth_lock = Mutex.new
    end

    def synchronize
      owned = @depth_lock.synchronize do
        @lock.owned?
      end

      if owned
        # The lock is owned by the current thread
        rv = yield

        owned = @depth_lock.synchronize do
          @lock.owned?
        end
        unless owned
          raise ThreadError, 'Unexpectedly lost ownership of the lock'
        end
      else
        # The lock is owned by another thread or not locked
        rv = @lock.synchronize do
          yield
        end

        owned = @depth_lock.synchronize do
          @lock.owned?
        end
        if owned
          raise ThreadError, 'Unexpectedly gained ownership of the lock'
        end
      end

      rv
    end
  end
end

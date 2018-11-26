# Copyright (C) 2018 MongoDB, Inc.
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
  # This is a semaphore implementation essentially encapsulating the
  # sample code at https://ruby-doc.org/stdlib-2.0.0/libdoc/thread/rdoc/ConditionVariable.html.
  #
  # @api private
  class Semaphore
    def initialize
      @lock = Mutex.new
      @cv = ConditionVariable.new
    end

    # Waits for the semaphore to be signaled up to timeout seconds.
    # If semaphore is not signaled, returns after timeout seconds.
    def wait(timeout)
      @lock.synchronize do
        @cv.wait(@lock, timeout)
      end
    end

    def broadcast
      @lock.synchronize do
        @cv.broadcast
      end
    end

    def signal
      @lock.synchronize do
        @cv.signal
      end
    end
  end
end

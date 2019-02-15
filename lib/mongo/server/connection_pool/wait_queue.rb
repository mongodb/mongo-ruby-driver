# Copyright (C) 2014-2019 MongoDB, Inc.
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
  class Server
    class ConnectionPool
      class WaitQueue
        extend Forwardable

        def initialize(address)
          @address = address
          @wait_queue = []
          @mutex = Mutex.new
        end

        def clear
          loop do
            semaphore = @mutex.synchronize do
              return if @wait_queue.empty?
              @wait_queue.shift
            end
            semaphore.broadcast
          end
        end

        def enter_wait_queue(wait_timeout, deadline)
          semaphore = (Thread::current[:_mongo_wait_queue_semaphore] ||= Semaphore.new)

          wait = @mutex.synchronize do
            @wait_queue << semaphore
            @wait_queue.size > 1
          end

          semaphore.wait(wait_timeout) if wait

          if deadline <= Time.now || @mutex.synchronize { !@wait_queue.include?(semaphore) }
            raise Error::ConnectionCheckoutTimeout.new(@address)
          end

          yield
        ensure
          @mutex.synchronize do
            @wait_queue.delete(semaphore)
            @wait_queue.first.broadcast unless @wait_queue.empty?
          end
        end
      end
    end
  end
end

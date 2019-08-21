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

  # The run!, running? and stop! methods used to be part of the public API
  # in some of the classes which now include this module. Therefore these
  # methods must be considered part of the driver's public API for backwards
  # compatibility reasons. However using these methods outside of the driver
  # is deprecated.
  #
  # @api private
  module BackgroundThread
    include Loggable

    # Start the background thread.
    #
    # If the thread is already running, this method does nothing.
    #
    # @api public for backwards compatibility only
    def run!
      if running?
        @thread
      else
        start!
      end
    end

    # @api public for backwards compatibility only
    def running?
      if @thread
        @thread.alive?
      else
        false
      end
    end

    # Stop the background thread and wait for to terminate for a reasonable
    # amount of time.
    #
    # @return [ true | false ] Whether the thread was terminated.
    #
    # @api public for backwards compatibility only
    def stop!
      # If the thread was not started, there is nothing to stop.
      #
      # Classes including this module may want to perform additional
      # cleanup, which they can do by overriding this method.
      return true unless @thread

      # Background threads generally perform operations in a loop.
      # This flag is meant to be checked on each iteration of the
      # working loops and the thread should stop working when this flag
      # is set.
      @stop_requested = true

      # Besides setting the flag, a particular class may have additional
      # ways of signaling the background thread to either stop working or
      # wake up to check the stop flag, for example, setting a semaphore.
      # This can be accomplished by providing the pre_stop method.
      pre_stop

      # Now we have requested the graceful termination, and we could wait
      # for the thread to exit on its own accord. A future version of the
      # driver may allow a certain amount of time for the thread to quit.
      # For now, we additionally use the Ruby machinery to request the thread
      # be terminated, and do so immediately.
      #
      # Note that this may cause the background thread to terminate in
      # the middle of an operation.
      @thread.kill

      # Wait for the thread to die. This is important in order to reliably
      # clean up resources like connections knowing that no background
      # thread will reconnect because it is still working.
      #
      # However, we do not want to wait indefinitely because in theory
      # a background thread could be performing, say, network I/O and if
      # the network is no longer available that could take a long time.
      start_time = Time.now
      ([0.1, 0.15] + [0.2] * 5 + [0.3] * 20).each do |interval|
        begin
          Timeout.timeout(interval) do
            @thread.join
          end
          break
        rescue Timeout::Error
        end
      end

      # Some driver objects can be reconnected, for backwards compatibiilty
      # reasons. Clear the thread instance variable to support this cleanly.
      if @thread.alive?
        log_warn("Failed to stop background thread in #{self} in #{(Time.now - start_time).to_i} seconds")
        false
      else
        @thread = nil
        true
      end
    end

    private

    def start!
      @thread = Thread.new do
        catch(:done) do
          until @stop_requested
            do_work
          end
        end
      end
    end

    # Override this method to do the work in the background thread.
    def do_work
    end

    # Override this method to perform additional signaling for the background
    # thread to stop.
    def pre_stop
    end
  end
end

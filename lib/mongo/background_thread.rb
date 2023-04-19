# frozen_string_literal: true
# rubocop:todo all

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

  # The run!, running? and stop! methods used to be part of the public API
  # in some of the classes which now include this module. Therefore these
  # methods must be considered part of the driver's public API for backwards
  # compatibility reasons. However using these methods outside of the driver
  # is deprecated.
  #
  # @note Do not start or stop background threads in finalizers. See
  #   https://jira.mongodb.org/browse/RUBY-2453 and
  #   https://bugs.ruby-lang.org/issues/16288. When interpreter exits,
  #   background threads are stopped first and finalizers are invoked next,
  #   and MRI's internal data structures are basically corrupt at this point
  #   if threads are being referenced. Prior to interpreter shutdown this
  #   means threads cannot be stopped by objects going out of scope, but
  #   most likely the threads hold references to said objects anyway if
  #   work is being performed thus the objects wouldn't go out of scope in
  #   the first place.
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
      if @stop_requested && @thread
        wait_for_stop
        if @thread.alive?
          log_warn("Starting a new background thread in #{self}, but the previous background thread is still running")
          @thread = nil
        end
        @stop_requested = false
      end
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

      wait_for_stop
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

    # Waits for the thread to die, with a timeout.
    #
    # Returns true if the thread died, false otherwise.
    def wait_for_stop
      # Wait for the thread to die. This is important in order to reliably
      # clean up resources like connections knowing that no background
      # thread will reconnect because it is still working.
      #
      # However, we do not want to wait indefinitely because in theory
      # a background thread could be performing, say, network I/O and if
      # the network is no longer available that could take a long time.
      start_time = Utils.monotonic_time
      ([0.1, 0.15] + [0.2] * 5 + [0.3] * 20).each do |interval|
        begin
          Timeout.timeout(interval) do
            @thread.join
          end
          break
        rescue ::Timeout::Error
        end
      end

      # Some driver objects can be reconnected, for backwards compatibiilty
      # reasons. Clear the thread instance variable to support this cleanly.
      if @thread.alive?
        log_warn("Failed to stop the background thread in #{self} in #{(Utils.monotonic_time - start_time).to_i} seconds: #{@thread.inspect} (thread status: #{@thread.status})")
        # On JRuby the thread may be stuck in aborting state
        # seemingly indefinitely. If the thread is aborting, consider it dead
        # for our purposes (we will create a new thread if needed, and
        # the background thread monitor will not detect the aborting thread
        # as being alive).
        if @thread.status == 'aborting'
          @thread = nil
          @stop_requested = false
        end
        false
      else
        @thread = nil
        @stop_requested = false
        true
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

# frozen_string_literal: true

module Mongo
  module DriverBench
    module Parallel
      # An implementation of a counter variable that can be waited on, which
      # will signal when the variable reaches zero.
      #
      # @api private
      class Counter
        # Create a new Counter object with the given initial value.
        #
        # @param [ Integer ] value the starting value of the counter (defaults
        #    to zero).
        def initialize(value = 0)
          @mutex = Thread::Mutex.new
          @condition = Thread::ConditionVariable.new
          @counter = value
        end

        # Describes a block where the counter is incremented before executing
        # it, and decremented afterward.
        #
        # @yield Calls the provided block with no arguments.
        def enter
          inc
          yield
        ensure
          dec
        end

        # Waits for the counter to be zero.
        def wait
          @mutex.synchronize do
            return if @counter.zero?
            @condition.wait(@mutex)
          end
        end

        # Increments the counter.
        def inc
          @mutex.synchronize { @counter += 1 }
        end

        # Decrements the counter. If the counter reaches zero,
        # a signal is sent to any waiting process.
        def dec
          @mutex.synchronize do
            @counter -= 1 if @counter > 0
            @condition.signal if @counter.zero?
          end
        end
      end
    end
  end
end

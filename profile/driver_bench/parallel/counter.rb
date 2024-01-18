# frozen_string_literal: true

require 'thread'

module Mongo
  module DriverBench
    module Parallel
      class Counter
        def initialize(value = 0)
          @mutex = Thread::Mutex.new
          @condition = Thread::ConditionVariable.new
          @counter = value
        end

        def enter
          inc
          yield
        ensure
          dec
        end

        def wait
          @mutex.synchronize do
            return if @counter.zero?
            @condition.wait(@mutex)
          end
        end

        def inc
          @mutex.synchronize { @counter += 1 }
        end

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

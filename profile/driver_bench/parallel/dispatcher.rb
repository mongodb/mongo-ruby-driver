# frozen_string_literal: true

require 'etc'

require_relative 'counter'

module Mongo
  module DriverBench
    module Parallel
      # Implements a dispatcher for executing multiple workers in parallel.
      #
      # @api private
      class Dispatcher
        attr_reader :source

        # Creates a new dispatcher with the given source. The source may be any
        # object that responds to ``#next``. It may be assumed that ``#next``
        # will be called in a thread-safe manner, so the source does not need
        # to worry about thread-safety in that regard. Each call to ``#next``
        # on the source object should return the next batch of work to be done.
        # When the source is empty, ``#next`` must return ``nil``.
        #
        # @param [ Object ] source an object responding to ``#next``.
        # @param [ Integer ] workers the number of workers to employ in
        #    performing the task.
        #
        # @yield The associated block is executed in each worker and must
        #    describe the worker's task to be accomplished.
        #
        # @yieldparam [ Object ] batch the next batch to be worked on.
        def initialize(source, workers: (ENV['WORKERS'] || (Etc.nprocessors * 0.4)).to_i, &block)
          @source = source
          @counter = Counter.new
          @source_mutex = Thread::Mutex.new

          @threads = Array.new(workers).map { Thread.new { @counter.enter { Thread.stop; worker_loop(&block) } } }
          sleep 0.1 until @threads.all? { |t| t.status == 'sleep' }
        end

        # Runs the workers and waits for them to finish.
        def run
          @threads.each(&:wakeup)
          @counter.wait
        end

        private

        # @return [ Object ] returns the next batch of work to be done (from
        #   the source object given when the dispatcher was created).
        def next_batch
          @source_mutex.synchronize do
            @source.next
          end
        end

        # Fetches the next batch and passes it to the block, in a loop.
        # Terminates when the next batch is ``nil``.
        def worker_loop(&block)
          loop do
            batch = next_batch or return
            block.call(batch)
          end
        end
      end
    end
  end
end

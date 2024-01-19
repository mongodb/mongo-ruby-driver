# frozen_string_literal: true

require 'etc'
require 'thread'

require_relative 'counter'

module Mongo
  module DriverBench
    module Parallel
      class Dispatcher
        attr_reader :source

        def initialize(source, workers: (ENV['WORKERS'] || (Etc.nprocessors * 0.4)).to_i, &block)
          @source = source
          @counter = Counter.new
          @source_mutex = Thread::Mutex.new

          @threads = Array.new(workers).map { Thread.new { @counter.enter { Thread.stop; worker_loop(&block) } } }
          sleep 0.1 until @threads.all? { |t| t.status == 'sleep' }
        end

        def run
          @threads.each(&:wakeup)
          @counter.wait
        end

        private

        def next_batch
          @source_mutex.synchronize do
            @source.next
          end
        end

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

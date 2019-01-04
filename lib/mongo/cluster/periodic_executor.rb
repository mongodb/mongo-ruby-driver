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

  class Cluster

    # A manager that calls #execute on its executors at a regular interval.
    #
    # @api private
    #
    # @since 2.5.0
    class PeriodicExecutor

      # The default time interval for the periodic executor to execute.
      #
      # @since 2.5.0
      FREQUENCY = 5

      # Create a periodic executor.
      #
      # @example Create a PeriodicExecutor.
      #   Mongo::Cluster::PeriodicExecutor.new(reaper, reaper2)
      #
      # @api private
      #
      # @since 2.5.0
      def initialize(*executors)
        @thread = nil
        @executors = executors
      end

      # Start the thread.
      #
      # @example Start the periodic executor's thread.
      #   periodic_executor.run!
      #
      # @api private
      #
      # @since 2.5.0
      def run!
        @thread && @thread.alive? ? @thread : start!
      end
      alias :restart! :run!

      # Stop the executor's thread.
      #
      # @example Stop the executors's thread.
      #   periodic_executor.stop!
      #
      # @param [ Boolean ] wait Whether to wait for background threads to
      #   finish running.
      #
      # @api private
      #
      # @since 2.5.0
      def stop!(wait=false)
        begin; flush; rescue; end
        @thread.kill
        if wait
          @thread.join
        end
        !@thread.alive?
      end

      # Trigger an execute call on each reaper.
      #
      # @example Trigger all reapers.
      #   periodic_executor.execute
      #
      # @api private
      #
      # @since 2.5.0
      def execute
        @executors.each(&:execute) and true
      end

      # Execute all pending operations.
      #
      # @example Execute all pending operations.
      #   periodic_executor.flush
      #
      # @api private
      #
      # @since 2.5.0
      def flush
        @executors.each(&:flush) and true
      end

      private

      def start!
        @thread = Thread.new(FREQUENCY) do |i|
          loop do
            sleep(i)
            execute
          end
        end
      end
    end
  end
end

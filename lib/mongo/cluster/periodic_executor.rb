# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2014-2020 MongoDB Inc.
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
      include BackgroundThread

      # The default time interval for the periodic executor to execute.
      #
      # @since 2.5.0
      FREQUENCY = 5

      # Create a periodic executor.
      #
      # @example Create a PeriodicExecutor.
      #   Mongo::Cluster::PeriodicExecutor.new([reaper, reaper2])
      #
      # @param [ Array<Object> ] executors The executors. Each must respond
      #   to #execute and #flush.
      # @param [ Hash ] options The options.
      #
      # @option options [ Logger ] :logger A custom logger to use.
      #
      # @api private
      def initialize(executors, options = {})
        @thread = nil
        @executors = executors
        @stop_semaphore = Semaphore.new
        @options = options
      end

      attr_reader :options

      alias :restart! :run!

      def do_work
        execute
        @stop_semaphore.wait(FREQUENCY)
      end

      def pre_stop
        @stop_semaphore.signal
      end

      def stop(final = false)
        super

        begin
          flush
        rescue
        end

        true
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
        @executors.each(&:execute)
        true
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
        @executors.each(&:flush)
        true
      end
    end
  end
end

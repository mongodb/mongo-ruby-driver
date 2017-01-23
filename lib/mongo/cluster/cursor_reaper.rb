# Copyright (C) 2014-2015 MongoDB, Inc.
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

require 'set'

module Mongo
  class Cluster

    # A manager that sends kill cursors operations at regular intervals to close
    # cursors that have been garbage collected without being exhausted.
    #
    # @since 2.3.0
    class CursorReaper
      extend Forwardable
      include Retryable

      # The default time interval for the cursor reaper to send pending kill cursors operations.
      #
      # @since 2.3.0
      FREQUENCY = 1.freeze

      # Create a cursor reaper.
      #
      # @example Create a CursorReaper.
      #   Mongo::Cluster::CursorReaper.new(cluster)
      #
      # @api private
      #
      # @since 2.3.0
      def initialize
        @to_kill = {}
        @active_cursors = Set.new
        @mutex = Mutex.new
        @thread = nil
      end

      # Start the cursor reaper's thread.
      #
      # @example Start the cursor reaper's thread.
      #   reaper.run!
      #
      # @api private
      #
      # @since 2.3.0
      def run!
        @thread && @thread.alive? ? @thread : start!
      end
      alias :restart! :run!

      # Schedule a kill cursors operation to be eventually executed.
      #
      # @example Schedule a kill cursors operation.
      #   cursor_reaper.schedule_kill_cursor(id, op_spec, server)
      #
      # @param [ Integer ] id The id of the cursor to kill.
      # @param [ Hash ] op_spec The spec for the kill cursors op.
      # @param [ Mongo::Server ] server The server to send the kill cursors operation to.
      #
      # @api private
      #
      # @since 2.3.0
      def schedule_kill_cursor(id, op_spec, server)
        @mutex.synchronize do
          if @active_cursors.include?(id)
            @to_kill[server] ||= Set.new
            @to_kill[server] << op_spec
          end
        end
      end

      # Register a cursor id as active.
      #
      # @example Register a cursor as active.
      #   cursor_reaper.register_cursor(id)
      #
      # @param [ Integer ] id The id of the cursor to register as active.
      #
      # @api private
      #
      # @since 2.3.0
      def register_cursor(id)
        if id && id > 0
          @mutex.synchronize do
            @active_cursors << id
          end
        end
      end

      # Unregister a cursor id, indicating that it's no longer active.
      #
      # @example Unregister a cursor.
      #   cursor_reaper.unregister_cursor(id)
      #
      # @param [ Integer ] id The id of the cursor to unregister.
      #
      # @api private
      #
      # @since 2.3.0
      def unregister_cursor(id)
        @mutex.synchronize do
          @active_cursors.delete(id)
        end
      end

      # Stop the cursor reaper's thread.
      #
      # @example Stop the cursor reaper's thread.
      #   reaper.stop!
      #
      # @api private
      #
      # @since 2.3.0
      def stop!
        @thread.kill && @thread.stop?
      end

      # Execute all pending kill cursors operations.
      #
      # @example Execute pending kill cursors operations.
      #   cursor_reaper.kill_cursors
      #
      # @api private
      #
      # @since 2.3.0
      def kill_cursors
        to_kill_copy = {}
        active_cursors_copy = []

        @mutex.synchronize do
          to_kill_copy = @to_kill.dup
          active_cursors_copy = @active_cursors.dup
          @to_kill = {}
        end

        to_kill_copy.each do |server, op_specs|
          op_specs.each do |op_spec|
            if server.features.find_command_enabled?
              Cursor::Builder::KillCursorsCommand.update_cursors(op_spec, active_cursors_copy.to_a)
              if Cursor::Builder::KillCursorsCommand.get_cursors_list(op_spec).size > 0
                Operation::Commands::Command.new(op_spec).execute(server)
              end
            else
              Cursor::Builder::OpKillCursors.update_cursors(op_spec, active_cursors_copy.to_a)
              if Cursor::Builder::OpKillCursors.get_cursors_list(op_spec).size > 0
                Operation::KillCursors.new(op_spec).execute(server)
              end
            end
          end
        end
      end

      private

      def start!
        @thread = Thread.new(FREQUENCY) do |i|
          loop do
            sleep(i)
            kill_cursors
          end
        end
      end
    end
  end
end

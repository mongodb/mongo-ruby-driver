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

    # A manager that sends kill cursors operations at regular intervals to close
    # cursors that have been garbage collected without being exhausted.
    #
    # @api private
    #
    # @since 2.3.0
    class CursorReaper
      include Retryable

      # The default time interval for the cursor reaper to send pending
      # kill cursors operations.
      #
      # @since 2.3.0
      FREQUENCY = 1.freeze

      # Create a cursor reaper.
      #
      # @param [ Cluster ] cluster The cluster.
      #
      # @api private
      def initialize(cluster)
        @cluster = cluster
        @to_kill = {}
        @active_cursor_ids = Set.new
        @mutex = Mutex.new
        @kill_spec_queue = Queue.new
      end

      attr_reader :cluster

      # Schedule a kill cursors operation to be eventually executed.
      #
      # @param [ Cursor::KillSpec ] kill_spec The kill specification.
      #
      # @api private
      def schedule_kill_cursor(kill_spec)
        @kill_spec_queue << kill_spec
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
        if id.nil?
          raise ArgumentError, 'register_cursor called with nil cursor_id'
        end
        if id == 0
          raise ArgumentError, 'register_cursor called with cursor_id=0'
        end

        @mutex.synchronize do
          @active_cursor_ids << id
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
        if id.nil?
          raise ArgumentError, 'unregister_cursor called with nil cursor_id'
        end
        if id == 0
          raise ArgumentError, 'unregister_cursor called with cursor_id=0'
        end

        @mutex.synchronize do
          @active_cursor_ids.delete(id)
        end
      end

      # Read and decode scheduled kill cursors operations.
      #
      # This method mutates instance variables without locking, so is is not
      # thread safe. Generally, it should not be called itself, this is a helper
      # for `kill_cursor` method.
      #
      # @api private
      def read_scheduled_kill_specs
        while kill_spec = @kill_spec_queue.pop(true)
          if @active_cursor_ids.include?(kill_spec.cursor_id)
            @to_kill[kill_spec.server_address] ||= Set.new
            @to_kill[kill_spec.server_address] << kill_spec
          end
        end
      rescue ThreadError
        # Empty queue, nothing to do.
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
        # TODO optimize this to batch kill cursor operations for the same
        # server/database/collection instead of killing each cursor
        # individually.
        loop do
          server_address = nil

          kill_spec = @mutex.synchronize do
            read_scheduled_kill_specs
            # Find a server that has any cursors scheduled for destruction.
            server_address, specs =
              @to_kill.detect { |_, specs| specs.any? }

            if specs.nil?
              # All servers have empty specs, nothing to do.
              return
            end

            # Note that this mutates the spec in the queue.
            # If the kill cursor operation fails, we don't attempt to
            # kill that cursor again.
            spec = specs.take(1).tap do |arr|
              specs.subtract(arr)
            end.first

            unless @active_cursor_ids.include?(spec.cursor_id)
              # The cursor was already killed, typically because it has
              # been iterated to completion. Remove the kill spec from
              # our records without doing any more work.
              spec = nil
            end

            spec
          end

          # If there was a spec to kill but its cursor was already killed,
          # look for another spec.
          next unless kill_spec

          # We could also pass kill_spec directly into the KillCursors
          # operation, though this would make that operation have a
          # different API from all of the other ones which accept hashes.
          spec = {
            cursor_ids: [kill_spec.cursor_id],
            coll_name: kill_spec.coll_name,
            db_name: kill_spec.db_name,
          }
          op = Operation::KillCursors.new(spec)

          server = cluster.servers.detect do |server|
            server.address == server_address
          end

          unless server
            # TODO We currently don't have a server for the address that the
            # cursor is associated with. We should leave the cursor in the
            # queue to be killed at a later time (when the server comes back).
            next
          end

          options = {
            server_api: server.options[:server_api],
            connection_global_id: kill_spec.connection_global_id,
          }
          op.execute(server, context: Operation::Context.new(options: options))

          if session = kill_spec.session
            if session.implicit?
              session.end_session
            end
          end
        end
      end
      alias :execute :kill_cursors
      alias :flush :kill_cursors
    end
  end
end

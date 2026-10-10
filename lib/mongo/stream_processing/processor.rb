# frozen_string_literal: true

# Copyright (C) 2026-present MongoDB Inc.
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
  module StreamProcessing
    # Handle for a specific named stream processor.
    #
    # Holding a handle does not imply the processor currently exists on the
    # server. Obtained via {Processors#get}.
    #
    # @since 2.25.0
    class Processor
      VALID_FAILOVER_MODES = %w[GRACEFUL FORCED].freeze

      # @return [ String ] The processor name.
      attr_reader :name

      # @param client [ Mongo::Client ] Workspace-bound client.
      # @param name [ String ]
      def initialize(client, name)
        raise ArgumentError, 'name must be non-empty' if name.nil? || name.empty?

        @client = client
        @admin = Mongo::Database.new(client, 'admin')
        @name = name
      end

      # Starts the processor. The processor MUST be in STOPPED or FAILED;
      # starting a STARTED processor returns an error from the server.
      #
      # @param opts [ Hash ] Options
      # @option opts [ Integer ] :workers Number of workers.
      # @option opts [ Boolean ] :clear_checkpoints Clear checkpoints before starting.
      # @option opts [ BSON::Timestamp ] :start_at_operation_time Resume from
      #   a specific operation time.
      # @option opts [ String ] :tier Compute tier. One of `"SP2"`, `"SP5"`,
      #   `"SP10"`, `"SP30"`, `"SP50"`.
      # @option opts [ Boolean ] :enable_auto_scaling Enable auto-scaling.
      # @option opts [ Hash ] :failover Failover configuration. Requires `:region`.
      #   Optional `:mode` ("GRACEFUL" / "FORCED") and `:dry_run`.
      def start(**opts)
        # NOTE: the spec's `startAfter` option is RESERVED for future use and is
        # not yet accepted by the server; this driver MUST NOT send it.
        cmd = { startStreamProcessor: @name }
        cmd[:workers] = opts[:workers] if opts.key?(:workers)

        sub = {}
        sub[:clearCheckpoints] = opts[:clear_checkpoints] if opts.key?(:clear_checkpoints)
        sub[:startAtOperationTime] = opts[:start_at_operation_time] if opts.key?(:start_at_operation_time)
        sub[:tier] = opts[:tier] if opts.key?(:tier)
        sub[:enableAutoScaling] = opts[:enable_auto_scaling] if opts.key?(:enable_auto_scaling)
        cmd[:options] = sub unless sub.empty?

        if opts.key?(:failover)
          failover = opts[:failover] || {}
          unless failover[:region].is_a?(String) && !failover[:region].empty?
            raise ArgumentError, ':failover requires a :region string'
          end

          if failover.key?(:mode) && !VALID_FAILOVER_MODES.include?(failover[:mode])
            raise ArgumentError,
                  "invalid :failover mode #{failover[:mode].inspect}; expected one of: " \
                  "#{VALID_FAILOVER_MODES.join(', ')}"
          end

          f = { region: failover[:region] }
          f[:mode] = failover[:mode] if failover.key?(:mode)
          f[:dryRun] = failover[:dry_run] if failover.key?(:dry_run)
          cmd[:failover] = f
        end

        run_command(cmd)
        nil
      end

      # Stops the processor. The processor remains in STOPPED and can be
      # restarted.
      def stop
        run_command(stopStreamProcessor: @name)
        nil
      end

      # Drops the processor permanently. A dropped processor cannot be
      # recovered.
      def drop
        run_command(dropStreamProcessor: @name)
        nil
      end

      # Returns runtime statistics for the processor. Returns an error from the
      # server if the processor is not in the STARTED state.
      #
      # @param opts [ Hash ]
      # @option opts [ Boolean ] :verbose Include per-operator statistics.
      # @return [ Hash ] The full stats response document.
      def stats(**opts)
        cmd = { getStreamProcessorStats: @name }
        cmd[:options] = { verbose: opts[:verbose] } if opts.key?(:verbose)
        run_command(cmd).documents.first
      end

      # Retrieves a batch of sampled documents.
      #
      # Routes to `startSampleStreamProcessor` when no `:cursor_id` is supplied
      # (or it is 0); otherwise routes to `getMoreSampleStreamProcessor` with
      # the supplied cursor id. The caller MUST stop iterating when the
      # returned {SamplesResult#cursor_id} is 0.
      #
      # @param opts [ Hash ]
      # @option opts [ Integer ] :cursor_id Cursor id from a prior call. Absent
      #   or 0 opens a new sample cursor.
      # @option opts [ Integer ] :limit Maximum docs to sample. Only sent on
      #   the initial call.
      # @option opts [ Integer ] :batch_size Documents per batch. Only sent on
      #   subsequent calls.
      # @return [ SamplesResult ]
      def samples(**opts)
        cursor_id = opts[:cursor_id].to_i

        if cursor_id.zero?
          cmd = { startSampleStreamProcessor: @name }
          cmd[:limit] = opts[:limit] if opts.key?(:limit)
          doc = run_command(cmd).documents.first || {}
          new_cursor_id = (doc['cursorId'] || 0).to_i
          if new_cursor_id.zero?
            raise Error::OperationFailure, 'startSampleStreamProcessor did not return a cursorId'
          end

          return SamplesResult.new(new_cursor_id, [])
        end

        cmd = { getMoreSampleStreamProcessor: @name, cursorId: cursor_id }
        cmd[:batchSize] = opts[:batch_size] if opts.key?(:batch_size)
        doc = run_command(cmd).documents.first || {}
        next_cursor_id = (doc['cursorId'] || 0).to_i
        # Dev-server deviation: some server builds use "messages" instead of
        # "nextBatch". Prefer the spec-defined "nextBatch" but fall back to
        # "messages" if present.
        batch = doc['nextBatch'] || doc['messages'] || []
        SamplesResult.new(next_cursor_id, batch)
      end

      private

      def run_command(cmd)
        @admin.command(cmd)
      end
    end
  end
end

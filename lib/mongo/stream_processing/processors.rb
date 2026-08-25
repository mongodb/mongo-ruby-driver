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
    # Handle for managing stream processors in a workspace.
    #
    # Obtained from {Mongo::StreamProcessing::Client#stream_processors}.
    #
    # @since 2.25.0
    class Processors
      # @param client [ Mongo::Client ] Workspace-bound client.
      def initialize(client)
        @client = client
        @admin = Mongo::Database.new(client, 'admin')
      end

      # Creates a new stream processor.
      #
      # @param name [ String ] Stream processor name.
      # @param pipeline [ Array<Hash> ] Aggregation pipeline.
      # @param opts [ Hash ] Options
      # @option opts [ Hash ] :dlq Dead letter queue configuration.
      # @option opts [ String ] :stream_meta_field_name Field name used for
      #   stream metadata.
      # @option opts [ String ] :tier Compute tier.
      # @option opts [ Boolean ] :failover Whether failover is enabled.
      def create(name, pipeline, **opts)
        raise ArgumentError, 'name must be non-empty' if name.nil? || name.empty?

        cmd = { createStreamProcessor: name, pipeline: pipeline }
        sub = {}
        sub[:dlq] = opts[:dlq] if opts.key?(:dlq)
        sub[:streamMetaFieldName] = opts[:stream_meta_field_name] if opts.key?(:stream_meta_field_name)
        sub[:tier] = opts[:tier] if opts.key?(:tier)
        sub[:failover] = opts[:failover] if opts.key?(:failover)
        cmd[:options] = sub unless sub.empty?

        @admin.command(cmd)
        nil
      end

      # Returns a handle for the named processor. Does not imply that the
      # processor currently exists on the server.
      #
      # @param name [ String ]
      # @return [ Processor ]
      def get(name)
        Processor.new(@client, name)
      end

      # Returns information about a single stream processor.
      #
      # Sends the `getStreamProcessor` command. Dev-server deviation: some
      # server builds wrap the processor document in a top-level `result` key.
      # This is unwrapped transparently.
      #
      # @param name [ String ]
      # @return [ ProcessorInfo ]
      def get_info(name)
        raise ArgumentError, 'name must be non-empty' if name.nil? || name.empty?

        doc = @admin.command(getStreamProcessor: name).documents.first || {}
        doc = doc['result'] if doc.is_a?(Hash) && doc['result'].is_a?(Hash)
        ProcessorInfo.new(doc)
      end
    end
  end
end

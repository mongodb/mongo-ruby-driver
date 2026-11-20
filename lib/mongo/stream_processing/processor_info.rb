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
    # Information about a single stream processor, returned by the
    # `getStreamProcessor` command.
    #
    # Fields the spec marks as Optional may be absent depending on server
    # version; the corresponding accessors return `nil` in that case.
    #
    # @since 2.25.0
    class ProcessorInfo
      # @return [ Hash ] The raw response document.
      attr_reader :raw

      # @param raw [ Hash ] The response document from `getStreamProcessor`.
      def initialize(raw)
        @raw = raw
      end

      # Processor id. Optional: not returned by all server versions.
      # @return [ String, nil ]
      def id
        @raw['id']
      end

      # @return [ String ]
      def name
        @raw['name']
      end

      # Current state. Per the ASP spec, drivers MUST surface unknown state
      # values as-is rather than mapping to a closed set, so this is returned
      # as a plain string.
      # @return [ String ]
      def state
        @raw['state']
      end

      # @return [ Array<Hash> ]
      def pipeline
        @raw['pipeline'] || []
      end

      # @return [ Integer, nil ] Optional: not returned by all server versions.
      def pipeline_version
        @raw['pipelineVersion']
      end

      # @return [ String, nil ]
      def tier
        @raw['tier']
      end

      # @return [ Hash, nil ]
      def dlq
        @raw['dlq']
      end

      # @return [ String, nil ]
      def stream_meta_field_name
        @raw['streamMetaFieldName']
      end

      # @return [ Boolean ]
      def auto_scaling_enabled?
        !!@raw['enableAutoScaling']
      end

      # @return [ Boolean ]
      def failover_enabled?
        !!@raw['failoverEnabled']
      end

      # @return [ String, nil ]
      def active_region
        @raw['activeRegion']
      end

      # @return [ String, nil ]
      def workspace_default_region
        @raw['workspaceDefaultRegion']
      end

      # @return [ BSON::Timestamp, Time, nil ]
      def last_state_change
        @raw['lastStateChange']
      end

      # @return [ BSON::Timestamp, Time, nil ]
      def last_modified_at
        @raw['lastModifiedAt']
      end

      # @return [ String, nil ]
      def modified_by
        @raw['modifiedBy']
      end

      # @return [ Boolean ]
      def started?
        !!@raw['hasStarted']
      end

      # Error message. Per spec this is always present; empty string indicates
      # no error has occurred.
      # @return [ String ]
      def error_msg
        @raw['errorMsg'] || ''
      end

      # @return [ Boolean ]
      def error_retryable?
        !!@raw['errorRetryable']
      end

      # @return [ Integer, nil ]
      def error_code
        @raw['errorCode']
      end

      # @return [ Object ] Field's value from the underlying document, or nil.
      def [](key)
        @raw[key.to_s]
      end
    end
  end
end

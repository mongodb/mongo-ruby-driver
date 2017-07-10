# Copyright (C) 2014-2017 MongoDB, Inc.
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
  class Error

    # Raised when an invalid server preference is provided.
    #
    # @since 2.0.0
    class InvalidServerPreference < Error

      # Error message when tags are specified for a read preference that cannot support them.
      #
      # @since 2.4.0
      NO_TAG_SUPPORT = 'This read preference cannot be combined with tags.'.freeze

      # Error message when a max staleness is specified for a read preference that cannot support it.
      #
      # @since 2.4.0
      NO_MAX_STALENESS_SUPPORT = 'max_staleness cannot be set for this read preference.'.freeze

      # Error message for when the max staleness is not at least twice the heartbeat frequency.
      #
      # @since 2.4.0
      INVALID_MAX_STALENESS = "`max_staleness` value is too small. It must be at least " +
        "`ServerSelector::SMALLEST_MAX_STALENESS_SECONDS` and (the cluster's heartbeat_frequency " +
          "setting + `Cluster::IDLE_WRITE_PERIOD_SECONDS`).".freeze

      # Error message when max staleness cannot be used because one or more servers has version < 3.4.
      #
      # @since 2.4.0
      NO_MAX_STALENESS_WITH_LEGACY_SERVER = 'max_staleness can only be set for a cluster in which ' +
                                              'each server is at least version 3.4.'.freeze

      # Instantiate the new exception.
      #
      # @example Instantiate the exception.
      #   Mongo::Error::InvalidServerPreference.new
      #
      # @param [ String ] message The error message.
      #
      # @since 2.0.0
      def initialize(message)
        super(message)
      end
    end
  end
end

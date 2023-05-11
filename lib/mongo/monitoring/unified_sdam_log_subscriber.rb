# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2019-2020 MongoDB Inc.
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

module Mongo
  class Monitoring

    # Subscribes to SDAM events and logs them.
    #
    # @since 2.11.0
    # @api experimental
    class UnifiedSdamLogSubscriber
      include Loggable

      # @return [ Hash ] options The options.
      #
      # @since 2.11.0
      attr_reader :options

      # Create the new log subscriber.
      #
      # @param [ Hash ] options The options.
      #
      # @option options [ Logger ] :logger An optional custom logger.
      #
      # @since 2.11.0
      def initialize(options = {})
        @options = options
      end

      # Handle an event.
      #
      # @param [ Event ] event The event.
      #
      # @since 2.11.0
      def published(event)
        log_debug("EVENT: #{event.summary}") if logger.debug?
      end

      alias :succeeded :published

      def subscribe(client)
        client.subscribe(Mongo::Monitoring::TOPOLOGY_OPENING, self)
        client.subscribe(Mongo::Monitoring::SERVER_OPENING, self)
        client.subscribe(Mongo::Monitoring::SERVER_DESCRIPTION_CHANGED, self)
        client.subscribe(Mongo::Monitoring::TOPOLOGY_CHANGED, self)
        client.subscribe(Mongo::Monitoring::SERVER_CLOSED, self)
        client.subscribe(Mongo::Monitoring::TOPOLOGY_CLOSED, self)
      end
    end
  end
end

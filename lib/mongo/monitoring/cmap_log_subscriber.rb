# Copyright (C) 2016-2019 MongoDB, Inc.
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

    # Subscribes to CMAP events and logs them.
    #
    # @since 2.8.0
    class CmapLogSubscriber
      include Loggable

      # @return [ Hash ] options The options.
      #
      # @since 2.8.0
      attr_reader :options

      # Create the new log subscriber.
      #
      # @example Create the log subscriber.
      #   CmapLogSubscriber.new
      #
      # @param [ Hash ] options The options.
      #
      # @option options [ Logger ] :logger An optional custom logger.
      #
      # @since 2.8.0
      def initialize(options = {})
        @options = options
      end

      # Handle the CMAP succeeded event.
      #
      # @example Handle the event.
      #   subscriber.succeeded(event)
      #
      # @param [ Event ] event The event.
      #
      # @since 2.8.0
      def succeeded(event)
        log_debug(event) if logger.debug?
      end
    end
  end
end

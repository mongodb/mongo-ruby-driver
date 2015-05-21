# Copyright (C) 2015 MongoDB, Inc.
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
  module Monitoring

    # Subscribes to command events and logs them.
    #
    # @since 2.1.0
    class CommandLogSubscriber

      # Handle the command started event.
      #
      # @example Handle the event.
      #   subscriber.started(event)
      #
      # @param [ CommandStartedEvent ] event The event.
      #
      # @since 2.1.0
      def started(event)
        Logger.logger.debug("MONGODB.#{event.name} STARTED | #{event.connection} | #{event.arguments}")
      end

      # Handle the command completed event.
      #
      # @example Handle the event.
      #   subscriber.completed(event)
      #
      # @param [ CommandCompletedEvent ] event The event.
      #
      # @since 2.1.0
      def completed(event)
        Logger.logger.debug("MONGODB.#{event.name} COMPLETED | #{event.connection} | (#{event.duration}s)")
      end

      # Handle the command failed event.
      #
      # @example Handle the event.
      #   subscriber.failed(event)
      #
      # @param [ CommandFailedEvent ] event The event.
      #
      # @since 2.1.0
      def failed(event)
        Logger.logger.debug("MONGODB.#{event.name} FAILED | #{event.connection} | #{event.message} | (#{event.duration}s)")
      end
    end
  end
end

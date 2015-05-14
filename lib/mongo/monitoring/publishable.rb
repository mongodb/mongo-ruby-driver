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

    # Defines behaviour for an object that can publish monitoring events.
    #
    # @since 2.1.0
    module Publishable

      # Publish an event to the global monitoring.
      #
      # @example Publish an event.
      #   object.publish(Monitoring::QUERY, { filter: { name: 'test' }})
      #
      # @param [ String ] topic The event topic.
      # @param [ Hash ] payload The event payload.
      #
      # @since 2.1.0
      def publish(messages)
        start = Time.now
        fire_events = Monitoring.subscribers?(Monitoring::COMMAND)
        if fire_events
          messages.each do |message|
            Monitoring.started(Monitoring::COMMAND, message.event(address.to_s))
          end
        end
        begin
          result = yield(messages)
          if result && fire_events
            Monitoring.completed(Monitoring::COMMAND, result.event(address.to_s, duration(start)))
          end
          result
        rescue Exception => e
          raise e
        end
      end

      private

      def duration(start)
        Time.now - start
      end
    end
  end
end

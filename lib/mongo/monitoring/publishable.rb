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
  class Monitoring

    # Defines behaviour for an object that can publish monitoring events.
    #
    # @since 2.1.0
    module Publishable

      # @return [ Monitoring ] monitoring The monitoring.
      attr_reader :monitoring

      # Publish a command event to the global monitoring.
      #
      # @example Publish a command event.
      #   publish_command do |messages|
      #     # ...
      #   end
      #
      # @param [ Array<Message> ] messages The messages.
      #
      # @return [ Object ] The result of the yield.
      #
      # @since 2.1.0
      def publish_command(messages)
        start = Time.now
        payload = messages.first.payload
        monitoring.started(
          Monitoring::COMMAND,
          Event::CommandStarted.generate(address, 1, payload)
        )
        begin
          result = yield(messages)
          monitoring.completed(
            Monitoring::COMMAND,
            Event::CommandCompleted.generate(
              address,
              1,
              payload,
              result ? result.payload : nil,
              duration(start)
            )
          )
          result
        rescue Exception => e
          monitoring.failed(
            Monitoring::COMMAND,
            Event::CommandFailed.generate(address, 1, payload, e.message, duration(start))
          )
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

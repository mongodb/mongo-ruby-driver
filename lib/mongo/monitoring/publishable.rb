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
      def publish_command(messages, operation_id = Monitoring.next_operation_id)
        start = Time.now
        payload = messages.first.payload
        command_started(address, operation_id, payload)
        begin
          result = yield(messages)
          command_completed(result, address, operation_id, payload, start)
          result
        rescue Exception => e
          command_failed(address, operation_id, payload, e.message, start)
          raise e
        end
      end

      private

      def command_started(address, operation_id, payload)
        monitoring.started(
          Monitoring::COMMAND,
          Event::CommandStarted.generate(address, operation_id, payload)
        )
      end

      def command_completed(result, address, operation_id, payload, start)
        document = result ? (result.documents || []).first : nil
        parser = Error::Parser.new(document)
        if parser.message.empty?
          command_succeeded(result, address, operation_id, payload, start)
        else
          command_failed(address, operation_id, payload, parser.message, start)
        end
      end

      def command_succeeded(result, address, operation_id, payload, start)
        monitoring.succeeded(
          Monitoring::COMMAND,
          Event::CommandSucceeded.generate(
            address,
            operation_id,
            payload,
            result ? result.payload : nil,
            duration(start)
          )
        )
      end

      def command_failed(address, operation_id, payload, message, start)
        monitoring.failed(
          Monitoring::COMMAND,
          Event::CommandFailed.generate(address, operation_id, payload, message, duration(start))
        )
      end

      def duration(start)
        Time.now - start
      end
    end
  end
end

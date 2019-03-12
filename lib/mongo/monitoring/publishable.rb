# Copyright (C) 2015-2019 MongoDB, Inc.
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

    # Defines behavior for an object that can publish monitoring events.
    #
    # @since 2.1.0
    module Publishable
      include Loggable

      # @return [ Monitoring ] monitoring The monitoring.
      attr_reader :monitoring

      def publish_event(topic, event)
        monitoring.succeeded(topic, event)
      end

      def publish_sdam_event(topic, event)
        return unless monitoring?

        log_debug("EVENT: #{event.summary}")
        monitoring.succeeded(topic, event)
      end

      private

      def command_started(address, operation_id, payload, socket_object_id = nil)
        monitoring.started(
          Monitoring::COMMAND,
          Event::CommandStarted.generate(address, operation_id, payload,
            socket_object_id)
        )
      end

      def command_completed(result, address, operation_id, payload, duration)
        document = result ? (result.documents || []).first : nil
        if error?(document)
          parser = Error::Parser.new(document)
          command_failed(document, address, operation_id, payload, parser.message, duration)
        else
          command_succeeded(result, address, operation_id, payload, duration)
        end
      end

      def command_succeeded(result, address, operation_id, payload, duration)
        monitoring.succeeded(
          Monitoring::COMMAND,
          Event::CommandSucceeded.generate(
            address,
            operation_id,
            payload,
            result ? result.payload : nil,
            duration
          )
        )
      end

      def command_failed(failure, address, operation_id, payload, message, duration)
        monitoring.failed(
          Monitoring::COMMAND,
          Event::CommandFailed.generate(address, operation_id, payload, message, failure, duration)
        )
      end

      def duration(start)
        Time.now - start
      end

      def error?(document)
        document && (document['ok'] == 0 || document.key?('$err'))
      end

      def monitoring?
        options[:monitoring] != false
      end
    end
  end
end

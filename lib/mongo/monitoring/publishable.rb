# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2015-2020 MongoDB Inc.
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

      # @deprecated
      def publish_event(topic, event)
        monitoring.succeeded(topic, event)
      end

      def publish_sdam_event(topic, event)
        return unless monitoring?

        monitoring.succeeded(topic, event)
      end

      def publish_cmap_event(event)
        return unless monitoring?

        monitoring.published(Monitoring::CONNECTION_POOL, event)
      end

      private

      def command_started(address, operation_id, payload,
        socket_object_id: nil, connection_id: nil, connection_generation: nil,
        server_connection_id: nil, service_id: nil
      )
        event = Event::CommandStarted.generate(address, operation_id, payload,
            socket_object_id: socket_object_id, connection_id: connection_id,
            connection_generation: connection_generation,
            server_connection_id: server_connection_id,
            service_id: service_id,
          )
        monitoring.started(
          Monitoring::COMMAND,
          event
        )
        event
      end

      def command_completed(result, address, operation_id, payload, duration,
        started_event:, server_connection_id: nil, service_id: nil
      )
        document = result ? (result.documents || []).first : nil
        if document && (document['ok'] && document['ok'] != 1 || document.key?('$err'))
          parser = Error::Parser.new(document)
          command_failed(document, address, operation_id,
            payload, parser.message, duration,
            started_event: started_event, server_connection_id: server_connection_id,
            service_id: service_id,
          )
        else
          command_succeeded(result, address, operation_id, payload, duration,
            started_event: started_event, server_connection_id: server_connection_id,
            service_id: service_id,
          )
        end
      end

      def command_succeeded(result, address, operation_id, payload, duration,
        started_event:, server_connection_id: nil, service_id: nil
      )
        monitoring.succeeded(
          Monitoring::COMMAND,
          Event::CommandSucceeded.generate(
            address,
            operation_id,
            payload,
            result ? result.payload : nil,
            duration,
            started_event: started_event,
            server_connection_id: server_connection_id,
            service_id: service_id,
          )
        )
      end

      def command_failed(failure, address, operation_id, payload, message, duration,
        started_event:, server_connection_id: nil, service_id: nil
      )
        monitoring.failed(
          Monitoring::COMMAND,
          Event::CommandFailed.generate(address, operation_id, payload,
            message, failure, duration,
            started_event: started_event,
            server_connection_id: server_connection_id,
            service_id: service_id,
          )
        )
      end

      def duration(start)
        Time.now - start
      end

      def monitoring?
        options[:monitoring] != false
      end
    end
  end
end

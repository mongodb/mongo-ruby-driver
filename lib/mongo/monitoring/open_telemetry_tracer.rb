# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2015-present MongoDB Inc.
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

    # Subscribes to command events and traces them to OpenTelemetry.
    #
    # @api private
    class OpenTelemetryTracer

      ENV_VARIABLE_ENABLED = 'OTEL_RUBY_INSTRUMENTATION_MONGODB_DISABLED'

      OTEL_TRACER_NAME = 'mongo-ruby-driver'

      def initialize(options = {})
        if defined?(::OpenTelemetry) && ENV[ENV_VARIABLE_ENABLED] != 'true'
          @tracer = (options[:opentelemetry_tracer_provider] || ::OpenTelemetry.tracer_provider).tracer(
              OTEL_TRACER_NAME, Mongo::VERSION
          )
        end
      end

      def in_span(message, operation, address)
        attributes = {
          'db.system' => 'mongodb',
          'db.name' => operation.spec[:db_name],
          'db.operation' => message.payload[:command_name],
          'net.peer.name' => address.host,
          'net.peer.port' => address.port
        }
        @tracer.in_span(span_name(message, operation), attributes: attributes) do |span|
          yield(span)
        end
      end

      def enabled?
        @tracer != nil
      end

      private

      def span_name(message, operation)
        collection = operation.spec[:coll_name]
        command_name = message.payload[:command_name]
        if collection
          "#{collection}.#{command_name}"
        else
          command_name
        end
      end
    end
  end
end

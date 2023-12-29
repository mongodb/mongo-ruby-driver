# frozen_string_literal: true

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
    module OpenTelemetry
      # This is a wrapper around OpenTelemetry tracer that provides a convenient
      # interface for creating spans.
      class Tracer
        ENV_VARIABLE_ENABLED = 'OTEL_RUBY_INSTRUMENTATION_MONGODB_DISABLED'

        ENV_VARIABLE_DB_STATEMENT = 'OTEL_RUBY_INSTRUMENTATION_MONGODB_DB_STATEMENT'

        DB_STATEMENT_DEFAULT_VALUE = 'obfuscate'

        DB_STATEMENT_VALUES = %i[omit obfuscate include].freeze

        OTEL_TRACER_NAME = 'mongo-ruby-driver'

        def initialize(options = {})
          return unless defined?(::OpenTelemetry) && ENV[ENV_VARIABLE_ENABLED] != 'true'

          @tracer = (options[:opentelemetry_tracer_provider] || ::OpenTelemetry.tracer_provider).tracer(
            OTEL_TRACER_NAME, Mongo::VERSION
          )
        end

        def in_span(message, operation, address, &block)
          @tracer.in_span(
            span_name(message, operation),
            attributes: attributes(message, operation, address),
            kind: :client,
            &block
          )
        end

        def enabled?
          @tracer != nil
        end

        private

        def db_statement
          @db_statement ||= ENV.fetch(ENV_VARIABLE_DB_STATEMENT, DB_STATEMENT_DEFAULT_VALUE).to_sym.tap do |statement|
            unless DB_STATEMENT_VALUES.include?(statement)
              raise ArgumentError, "Invalid value for #{ENV_VARIABLE_DB_STATEMENT}: #{statement}"
            end
          end
        end

        def omit?
          db_statement == :omit
        end

        def obfuscate?
          db_statement == :obfuscate
        end

        def span_name(message, operation)
          collection = operation.spec[:coll_name]
          command_name = message.payload[:command_name]
          if collection
            "#{collection}.#{command_name}"
          else
            command_name
          end
        end

        def attributes(message, operation, address)
          {
            'db.system' => 'mongodb',
            'db.name' => message.payload[:database_name],
            'db.operation' => message.payload[:command_name],
            'net.peer.name' => address.host,
            'net.peer.port' => address.port,
            'db.mongodb.collection' => operation.spec[:coll_name],
          }.tap do |attributes|
            attributes['db.statement'] = StatementBuilder.new(message, obfuscate?).build unless omit?
          end
        end
      end
    end
  end
end

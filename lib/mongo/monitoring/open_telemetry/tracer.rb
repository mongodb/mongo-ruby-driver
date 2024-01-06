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
        # Environment variable that enables otel instrumentation.
        ENV_VARIABLE_ENABLED = 'OTEL_RUBY_INSTRUMENTATION_MONGODB_ENABLED'

        # Environment variable that controls the db.statement attribute.
        # Possible values are:
        # - omit: do not include db.statement attribute
        # - obfuscate: obfuscate the attribute value
        # - include: include the attribute value as is
        # Default value is obfuscate.
        ENV_VARIABLE_DB_STATEMENT = 'OTEL_RUBY_INSTRUMENTATION_MONGODB_DB_STATEMENT'

        # Default value for db.statement attribute.
        DB_STATEMENT_DEFAULT_VALUE = 'obfuscate'

        # Possible values for db.statement attribute.
        DB_STATEMENT_VALUES = %i[omit obfuscate include].freeze

        # Name of the tracer.
        OTEL_TRACER_NAME = 'mongo-ruby-driver'

        # @return [ OpenTelemetry::SDK::Trace::Tracer | nil ] The otel tracer.
        attr_reader :ot_tracer

        def initialize
          return unless defined?(::OpenTelemetry)
          return unless ENV[ENV_VARIABLE_ENABLED] == 'true'

          @ot_tracer = ::OpenTelemetry.tracer_provider.tracer(
            OTEL_TRACER_NAME,
            Mongo::VERSION
          )
        end

        # If otel instrumentation is enabled, creates a span with attributes
        # for the message and operation and yields it to the block.
        # Otherwise, yields to the block.
        #
        # @param [ Protocol::Message ] message The message.
        # @param [ Operation ] operation The operation.
        # @param [ Address ] address The address of the server the message is sent to.
        # @param [ Proc ] &block The block to be executed.
        def in_span(message, operation, address, &block)
          if enabled?
            @ot_tracer.in_span(
              span_name(message, operation),
              attributes: attributes(message, operation, address),
              kind: :client,
              &block
            )
          else
            yield
          end
        end

        private

        # @return [ true | false ] Whether otel instrumentation is enabled.
        def enabled?
          @ot_tracer != nil
        end

        # Validates and returns the value of db.statement attribute of the span.
        #
        # @return [ Symbol ] The value of db.statement attribute.
        # @raise [ ArgumentError ] If the value is invalid.
        def db_statement
          @db_statement ||= ENV.fetch(ENV_VARIABLE_DB_STATEMENT, DB_STATEMENT_DEFAULT_VALUE).to_sym.tap do |statement|
            unless DB_STATEMENT_VALUES.include?(statement)
              raise ArgumentError, "Invalid value for #{ENV_VARIABLE_DB_STATEMENT}: #{statement}"
            end
          end
        end

        # @return [ true | false ] Whether db.statement attribute should be omitted.
        def omit?
          db_statement == :omit
        end

        # @return [ true | false ] Whether db.statement attribute should be obfuscated.
        def obfuscate?
          db_statement == :obfuscate
        end

        # @return [ String ] The name of the span.
        def span_name(message, operation)
          collection = operation.spec[:coll_name]
          command_name = message.payload[:command_name]
          if collection
            "#{collection}.#{command_name}"
          else
            command_name
          end
        end

        # @return [ Hash ] The attributes of the span.
        def attributes(message, operation, address)
          {
            'db.system' => 'mongodb',
            'db.name' => message.payload[:database_name],
            'db.operation' => message.payload[:command_name],
            'net.peer.name' => address.host,
            'net.peer.port' => address.port,
          }.tap do |attributes|
            attributes['db.mongodb.collection'] = operation.spec[:coll_name] unless operation.spec[:coll_name].nil?
            attributes['db.statement'] = StatementBuilder.new(message.payload[:command], obfuscate?).build unless omit?
          end
        end
      end
    end
  end
end

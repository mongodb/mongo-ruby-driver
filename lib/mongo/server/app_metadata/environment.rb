# frozen_string_literal: true

# Copyright (C) 2016-2020 MongoDB Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

require 'mongo/server/app_metadata/environment'

module Mongo
  class Server
    class AppMetadata
      # @api private
      class Environment
        # Error class for reporting that too many discriminators were found
        # in the environment. (E.g. if the environment reports that it is
        # running under both AWS and Azure.)
        class TooManyEnvironments < Mongo::Error; end

        # Error class for reporting that a required environment variable is
        # missing.
        class MissingVariable < Mongo::Error; end

        # Error class for reporting that the wrong type was given for a
        # field.
        class TypeMismatch < Mongo::Error; end

        # Error class for reporting that the value for a field is too long.
        class ValueTooLong < Mongo::Error; end

        # This value is not explicitly specified in the spec, only implied to be
        # less than 512.
        MAXIMUM_VALUE_LENGTH = 500

        DISCRIMINATORS = {
          'AWS_EXECUTION_ENV'        => 'aws.lambda',
          'AWS_LAMBDA_RUNTIME_API'   => 'aws.lambda',
          'FUNCTIONS_WORKER_RUNTIME' => 'azure.func',
          'K_SERVICE'                => 'gcp.func',
          'FUNCTION_NAME'            => 'gcp.func',
          'VERCEL'                   => 'vercel',
        }.freeze

        COERCIONS = {
          string:  -> (v) { String(v) },
          integer: -> (v) { Integer(v) }
        }

        FIELDS = {
          'aws.lambda' => {
            'AWS_REGION' => { field: :region, type: :string },
            'AWS_LAMBDA_FUNCTION_MEMORY_SIZE' => { field: :memory_mb, type: :integer },
          },

          'azure.func' => {},

          'gcp.func' => {
            'FUNCTION_MEMORY_MB' => { field: :memory_mb, type: :integer },
            'FUNCTION_TIMEOUT_SEC' => { field: :timeout_sec, type: :integer },
            'FUNCTION_REGION' => { field: :region, type: :string },
          },

          'vercel' => {
            'VERCEL_URL' => { field: :url, type: :string },
            'VERCEL_REGION' => { field: :region, type: :string },
          },
        }

        attr_reader :name
        attr_reader :fields
        attr_reader :error

        def initialize
          @error = nil
          @name = detect_environment
          populate_fields
        rescue TooManyEnvironments => e
          set_error "too many environments detected: #{e.message}"
        rescue MissingVariable => e
          set_error "missing environment variable: #{e.message}"
        rescue TypeMismatch => e
          set_error e.message
        rescue ValueTooLong => e
          set_error "value for #{e.message} is too long"
        end

        def faas?
          @name != nil
        end

        def aws?
          @name == 'aws.lambda'
        end

        def azure?
          @name == 'azure.func'
        end

        def gcp?
          @name == 'gcp.func'
        end

        def vercel?
          @name == 'vercel'
        end

        def to_h
          fields.merge(name: name)
        end

        private

        def detect_environment
          matches = DISCRIMINATORS.keys.select { |k| ENV[k] }
          names = matches.map { |m| DISCRIMINATORS[m] }.uniq

          raise TooManyEnvironments, names.join(", ") if names.length > 1

          names.first
        end

        def populate_fields
          return unless name

          @fields = FIELDS[name].each_with_object({}) do |(var, defn), fields|
            raise MissingVariable, var unless ENV[var]

            raise ValueTooLong, var if ENV[var].length > MAXIMUM_VALUE_LENGTH
            coerced = COERCIONS[defn[:type]].call(ENV[var])

            fields[defn[:field]] = coerced
          rescue ArgumentError
            raise TypeMismatch, "#{var} must be #{defn[:type]} (got #{ENV[var].inspect})"
          end
        end

        def set_error(msg)
          @name = nil
          @error = msg
        end
      end
    end
  end
end

# frozen_string_literal: true

# Copyright (C) 2016-2023 MongoDB Inc.
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

module Mongo
  class Server
    class AppMetadata
      # Implements the logic from the handshake spec, for deducing and
      # reporting the current FaaS environment in which the program is
      # executing.
      #
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

        # The mapping that determines which FaaS environment is active, based
        # on which environment variable(s) are present.
        DISCRIMINATORS = {
          'AWS_EXECUTION_ENV' => { pattern: /^AWS_Lambda_/, name: 'aws.lambda' },
          'AWS_LAMBDA_RUNTIME_API' => { name: 'aws.lambda' },
          'FUNCTIONS_WORKER_RUNTIME' => { name: 'azure.func' },
          'K_SERVICE' => { name: 'gcp.func' },
          'FUNCTION_NAME' => { name: 'gcp.func' },
          'VERCEL' => { name: 'vercel' },
        }.freeze

        # Describes how to coerce values of the specified type.
        COERCIONS = {
          string: ->(v) { String(v) },
          integer: ->(v) { Integer(v) }
        }.freeze

        # Describes which fields are required for each FaaS environment,
        # along with their expected types, and how they should be named in
        # the handshake document.
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
            'VERCEL_REGION' => { field: :region, type: :string },
          },
        }.freeze

        # @return [ String | nil ] the name of the FaaS environment that was
        #   detected, or nil if no valid FaaS environment was detected.
        attr_reader :name

        # @return [ Hash | nil ] the fields describing the detected FaaS
        #   environment.
        attr_reader :fields

        # @return [ String | nil ] the error message explaining why a valid
        #   FaaS environment was not detected, or nil if no error occurred.
        #
        # @note These error messagess are not to be propogated to the
        #   user; they are intended only for troubleshooting and debugging.)
        attr_reader :error

        # Create a new AppMetadata::Environment object, initializing it from
        # the current ENV variables. If no FaaS environment is detected, or
        # if the environment contains invalid or contradictory state, it will
        # be initialized with {{name}} set to {{nil}}.
        def initialize
          @error = nil
          @name = detect_environment
          populate_fields
        rescue TooManyEnvironments => e
          self.error = "too many environments detected: #{e.message}"
        rescue MissingVariable => e
          self.error = "missing environment variable: #{e.message}"
        rescue TypeMismatch => e
          self.error = e.message
        rescue ValueTooLong => e
          self.error = "value for #{e.message} is too long"
        end

        # Queries whether the current environment is a valid FaaS environment.
        #
        # @return [ true | false ] whether the environment is a FaaS
        #   environment or not.
        def faas?
          @name != nil
        end

        # Queries whether the current environment is a valid AWS Lambda
        # environment.
        #
        # @return [ true | false ] whether the environment is a AWS Lambda
        #   environment or not.
        def aws?
          @name == 'aws.lambda'
        end

        # Queries whether the current environment is a valid Azure
        # environment.
        #
        # @return [ true | false ] whether the environment is a Azure
        #   environment or not.
        def azure?
          @name == 'azure.func'
        end

        # Queries whether the current environment is a valid GCP
        # environment.
        #
        # @return [ true | false ] whether the environment is a GCP
        #   environment or not.
        def gcp?
          @name == 'gcp.func'
        end

        # Queries whether the current environment is a valid Vercel
        # environment.
        #
        # @return [ true | false ] whether the environment is a Vercel
        #   environment or not.
        def vercel?
          @name == 'vercel'
        end

        # Compiles the detected environment information into a Hash. It will
        # always include a {{name}} key, but may include other keys as well,
        # depending on the detected FaaS environment. (See the handshake
        # spec for details.)
        #
        # @return [ Hash ] the detected environment information.
        def to_h
          fields.merge(name: name)
        end

        private

        # Searches the DESCRIMINATORS list to see which (if any) apply to
        # the current environment.
        #
        # @return [ String | nil ] the name of the detected FaaS provider.
        #
        # @raise [ TooManyEnvironments ] if the environment contains
        #   discriminating variables for more than one FaaS provider.
        def detect_environment
          matches = DISCRIMINATORS.keys.select { |k| discriminator_matches?(k) }
          names = matches.map { |m| DISCRIMINATORS[m][:name] }.uniq

          # From the spec:
          # When variables for multiple ``client.env.name`` values are present,
          # ``vercel`` takes precedence over ``aws.lambda``; any other
          # combination MUST cause ``client.env`` to be entirely omitted.
          return 'vercel' if names.sort == %w[ aws.lambda vercel ]
          raise TooManyEnvironments, names.join(', ') if names.length > 1

          names.first
        end

        # Determines whether the named environment variable exists, and (if
        # a pattern has been declared for that descriminator) whether the
        # pattern matches the value of the variable.
        #
        # @param [ String ] var the name of the environment variable
        #
        # @return [ true | false ] if the variable describes the current
        #   environment or not.
        def discriminator_matches?(var)
          return false unless ENV[var]

          disc = DISCRIMINATORS[var]
          return true unless disc[:pattern]

          disc[:pattern].match?(ENV[var])
        end

        # Extracts environment information from the current environment
        # variables, based on the detected FaaS environment. Populates the
        # {{@fields}} instance variable.
        def populate_fields
          return unless name

          @fields = FIELDS[name].each_with_object({}) do |(var, defn), fields|
            fields[defn[:field]] = extract_field(var, defn)
          end
        end

        # Extracts the named variable from the environment and validates it
        # against its declared definition.
        #
        # @param [ String ] var The name of the environment variable to look
        #   for.
        # @param [ Hash ] definition The definition of the field that applies
        #   to the named variable.
        #
        # @return [ Integer | String ] the validated and coerced value of the
        #   given environment variable.
        #
        # @raise [ MissingVariable ] if the environment does not include a
        #   variable required by the current FaaS provider.
        # @raise [ ValueTooLong ] if a required variable is too long.
        # @raise [ TypeMismatch ] if a required variable cannot be coerced to
        #   the expected type.
        def extract_field(var, definition)
          raise MissingVariable, var unless ENV[var]
          raise ValueTooLong, var if ENV[var].length > MAXIMUM_VALUE_LENGTH

          COERCIONS[definition[:type]].call(ENV[var])
        rescue ArgumentError
          raise TypeMismatch,
                "#{var} must be #{definition[:type]} (got #{ENV[var].inspect})"
        end

        # Sets the error message to the given value and sets the name to nil.
        #
        # @param [ String ] msg The error message to store.
        def error=(msg)
          @name = nil
          @error = msg
        end
      end
    end
  end
end

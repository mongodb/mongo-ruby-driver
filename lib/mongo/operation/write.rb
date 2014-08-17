# Copyright (C) 2009-2014 MongoDB, Inc.
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

require 'mongo/operation/write/delete'
require 'mongo/operation/write/insert'
require 'mongo/operation/write/update'
require 'mongo/operation/write/drop_index'
require 'mongo/operation/write/ensure_index'
require 'mongo/operation/write/write_command'

module Mongo
  module Operation
    module Write

      # The write errors field in the response, 2.6 and higher.
      #
      # @since 2.0.0
      WRITE_ERRORS = 'writeErrors'.freeze

      # Constant for the errmsg field.
      #
      # @since 2.0.0
      ERROR_MESSAGE = 'errmsg'.freeze

      # The write concern error field in the response. 2.4 and lower.
      #
      # @since 2.0.0
      WRITE_CONCERN_ERROR = 'writeConcernError'.freeze

      # Raised when a write failes for some reason.
      #
      # @since 2.0.0
      class Failure < RuntimeError

        attr_reader :document

        # Initialize the exception with the document that triggered the error.
        #
        # @example Initialize the new exception.
        #   Write::Failure.new({ 'ok' => 0.0 })
        #
        # @param [ Hash ] document The document that triggered the error.
        #
        # @since 2.0.0
        def initialize(document)
          p document
          @document = document
          super(generate_message)
        end

        private

        def errors
          error_message(Operation::ERROR) do
            "#{document[Operation::ERROR_CODE]}: #{document[Operation::ERROR]}"
          end
        end

        def error_messages
          error_message(ERROR_MESSAGE) do
            document[ERROR_MESSAGE]
          end
        end

        def error_message(key)
          document.has_key?(key) ? yield : ''
        end

        def generate_message
          errors + error_messages + write_errors + write_concern_errors
        end

        def write_errors
          error_message(WRITE_ERRORS) do
            document[WRITE_ERRORS].map do |e|
              "#{e[Operation::ERROR_CODE]}: #{e[ERROR_MESSAGE]}"
            end.join(', ')
          end
        end

        def write_concern_errors
          error_message(WRITE_CONCERN_ERROR) do
            document[WRITE_CONCERN_ERROR].map do |e|
              "#{e[Operation::ERROR_CODE]}: #{e[ERROR_MESSAGE]}"
            end.join(', ')
          end
        end
      end
    end
  end
end

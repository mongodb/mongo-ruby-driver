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
require 'mongo/operation/write/write_command'

module Mongo
  module Operation
    module Write

      # The write errors field in the response, 2.6 and higher.
      #
      # @since 2.0.0
      ERRORS = 'writeErrors'.freeze

      # Constant for the error code field.
      #
      # @since 2.0.0
      ERROR_CODE = 'code'.freeze

      # Constant for the errmsg field.
      #
      # @since 2.0.0
      ERROR_MESSAGE = 'errmsg'.freeze

      # The write concern error field in the response. 2.4 and lower.
      #
      # @since 2.0.0
      CONCERN_ERROR = 'writeConcernError'.freeze

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
          @document = document
          super(generate_message)
        end

        private

        def generate_message
          errors = document[ERRORS] || document[CONCERN_ERROR]
          errors.map{ |e| "Error Code: #{e[ERROR_CODE]} | #{e[ERROR_MESSAGE]}" }.join(', ')
        end
      end
    end
  end
end

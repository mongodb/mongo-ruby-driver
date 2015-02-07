# Copyright (C) 2014-2015 MongoDB, Inc.
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
require 'mongo/operation/write/bulk_delete'
require 'mongo/operation/write/insert'
require 'mongo/operation/write/bulk_insert'
require 'mongo/operation/write/update'
require 'mongo/operation/write/bulk_update'
require 'mongo/operation/write/create_index'
require 'mongo/operation/write/drop_index'
require 'mongo/operation/write/create_user'
require 'mongo/operation/write/remove_user'
require 'mongo/operation/write/command'

module Mongo
  module Operation
    module Write

      # Raised when a write failes for some reason.
      #
      # @since 2.0.0
      class Failure < Error::OperationFailure

        # @return [ BSON::Document] document The error document.
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
          super(Error::Parser.new(document).parse)
        end
      end
    end
  end
end

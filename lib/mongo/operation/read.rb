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

require 'mongo/operation/read/query'
require 'mongo/operation/read/get_more'
require 'mongo/operation/read/indexes'
require 'mongo/operation/read/list_indexes'
require 'mongo/operation/read/list_collections'
require 'mongo/operation/read/collections_info'

module Mongo
  module Operation
    module Read

      # Raised for general errors happening on reads.
      #
      # @since 2.0.0
      class Failure < OperationError

        # Initialize the failure error.
        #
        # @example Initialize the error.
        #   Failure.new(document)
        #
        # @param [ BSON::Document ] document The document.
        #
        # @since 2.0.0
        def initialize(document)
          super(document[ERROR_MSG])
        end
      end

      # Raised for read commands that attempt to execute on a collection or
      # database that does not exist.
      #
      # @since 2.0.0
      class NoNamespace < OperationError

        # Initialize the exception.
        #
        # @example Initialize the exception.
        #   NoNamespace.new(document, spec)
        #
        # @param [ BSON::Document ] document The error document.
        # @param [ Hash ] spec The spec the command executed with.
        #
        # @since 2.0.0
        def initialize(document, spec)
          super(
            "Command failed with '#{document[ERROR_MSG]}' " +
            "for collection '#{spec[:coll_name]}' on database '#{spec[:db_name]}'."
          )
        end
      end
    end
  end
end

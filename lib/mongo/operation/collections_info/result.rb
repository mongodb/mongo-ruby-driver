# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2014-2020 MongoDB Inc.
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
  module Operation
    class CollectionsInfo

      # Defines custom behavior of results when query the system.namespaces
      # collection.
      #
      # @since 2.1.0
      # @api semiprivate
      class Result < Operation::Result

        # Initialize a new result.
        #
        # @param [ Array<Protocol::Message> | nil ] replies The wire protocol replies, if any.
        # @param [ Server::Description ] connection_description
        #   Server description of the server that performed the operation that
        #   this result is for.
        # @param [ Integer ] connection_global_id
        #   Global id of the connection on which the operation that
        #   this result is for was performed.
        # @param [ String ] database_name The name of the database that the
        #   query was sent to.
        #
        # @api private
        def initialize(replies, connection_description, connection_global_id, database_name)
          super(replies, connection_description, connection_global_id)
          @database_name = database_name
        end

        # Get the namespace for the cursor.
        #
        # @example Get the namespace.
        #   result.namespace
        #
        # @return [ String ] The namespace.
        #
        # @since 2.1.0
        # @api private
        def namespace
          "#{@database_name}.#{Database::NAMESPACES}"
        end
      end
    end
  end
end

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

require 'mongo/operation/write/drop_index/response'

module Mongo
  module Operation
    module Write

      # A MongoDB drop index operation.
      # If a server with version >= 2.5.5 is being used, a write command operation
      # will be created and sent instead.
      #
      # @since 2.0.0
      class DropIndex
        include Executable

        # Initialize the drop index operation.
        #
        # @example
        #   Write::DropIndex.new({
        #     :db_name       => 'test',
        #     :coll_name     => 'test_coll',
        #     :index_name    => 'name_1_age_-1'
        #   })
        #
        # @param [ Hash ] spec The specifications for the drop.
        #
        # @option spec :index [ Hash ] The index spec to create.
        # @option spec :db_name [ String ] The name of the database.
        # @option spec :coll_name [ String ] The name of the collection.
        # @option spec :index_name [ String ] The name of the index.
        #
        # @since 2.0.0
        def initialize(spec)
          @spec = spec
        end

        # Execute the operation.
        # If the server has version < 2.5.5, an insert operation is sent.
        # If the server version is >= 2.5.5, an insert write command operation is created
        # and sent instead.
        #
        # @params [ Mongo::Server::Context ] The context for this operation.
        #
        # @return [ Mongo::Response ] The operation response, if there is one.
        #
        # @since 2.0.0
        def execute(context)
          unless context.primary? || context.standalone?
            raise Exception, "Must use primary server to drop an index."
          end
          Response.new(Command::DropIndex.new(spec).execute(context)).verify!
        end
      end
    end
  end
end

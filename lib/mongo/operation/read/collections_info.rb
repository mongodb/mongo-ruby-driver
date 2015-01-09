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

module Mongo
  module Operation
    module Read

      # A MongoDB operation to get a list of collection names in a database.
      #
      # @example Create the collection names operation.
      #   Read::CollectionNames.new(:db_name => 'test-db')
      #
      # @param [ Hash ] spec The specifications for the collection names operation.
      #
      # @option spec :db_name [ String ] The name of the database whose collection
      #   names is requested.
      # @option spec :options [ Hash ] Options for the operation.
      #
      # @since 2.0.0
      class CollectionsInfo
        include Executable
        include Specifiable

        # Execute the operation.
        # The context gets a connection on which the operation
        # is sent in the block.
        #
        # @params [ Mongo::Server::Context ] The context for this operation.
        #
        # @return [ Result ] The operation response, if there is one.
        #
        # @since 2.0.0
        def execute(context)
          if context.features.list_collections_enabled?
            ListCollections.new(spec).execute(context)
          else
            context.with_connection do |connection|
              Result.new(connection.dispatch([ message ]))
            end
          end
        end

        private

        def selector
          { :name => { '$not' => /system\.|\$/ } }
        end

        def message
          Protocol::Query.new(db_name, Mongo::Database::NAMESPACES, selector, options)
        end
      end
    end
  end
end

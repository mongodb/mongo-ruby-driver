# Copyright (C) 2014-2017 MongoDB, Inc.
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
    module Commands

      # A MongoDB get indexes operation.
      #
      # Initialize the get indexes operation.
      #
      # @example Instantiate the operation.
      #   Read::Indexes.new(:db_name => 'test', :coll_name => 'test_coll')
      #
      # Initialization:
      #   param [ Hash ] spec The specifications for the insert.
      #
      #   option spec :db_name [ String ] The name of the database.
      #   option spec :coll_name [ String ] The name of the collection.
      #
      # @since 2.0.0
      class Indexes
        include Specifiable
        include ReadPreference

        # Execute the operation.
        #
        # @example Execute the operation.
        #   operation.execute(server)
        #
        # @param [ Mongo::Server ] server The server to send this operation to.
        #
        # @return [ Result ] The indexes operation response.
        #
        # @since 2.0.0
        def execute(server)
          if server.features.list_indexes_enabled?
            ListIndexes.new(spec).execute(server)
          else
            execute_message(server)
          end
        end

        private

        def execute_message(server)
          server.with_connection do |connection|
            Result.new(connection.dispatch([ message(server) ]))
          end
        end

        def selector
          { ns: namespace }
        end

        def query_coll
          Index::COLLECTION
        end
      end
    end
  end
end

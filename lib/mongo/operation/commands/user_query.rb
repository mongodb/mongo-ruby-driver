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

      # A MongoDB operation to get info of a particular user in a database.
      #
      # @example Create the user query operation.
      #   Read::UserQuery.new(:name => 'emily', :db_name => 'test-db')
      #
      # Initialization:
      #   param [ Hash ] spec The specifications for the user query operation.
      #
      #   option spec :user_name [ String ] The name of the user.
      #   option spec :db_name [ String ] The name of the database where the user exists.
      #   option spec :options [ Hash ] Options for the operation.
      #
      # @since 2.1.0
      class UserQuery
        include Executable
        include Specifiable

        # Execute the operation.
        #
        # @example Execute the operation.
        #   operation.execute(server)
        #
        # @param [ Mongo::Server ] server The server to send this operation to.
        #
        # @return [ Result ] The operation response, if there is one.
        #
        # @since 2.1.0
        def execute(server)
          if server.features.users_info_enabled?
            UsersInfo.new(spec).execute(server).validate!
          else
            server.with_connection do |connection|
              Result.new(connection.dispatch([ message(server) ])).validate!
            end
          end
        end

        private

        def selector
          { :user => user_name }
        end

        def query_coll
          Auth::User::COLLECTION
        end

        def message(server)
          Protocol::Query.new(db_name, query_coll, selector, options)
        end
      end
    end
  end
end

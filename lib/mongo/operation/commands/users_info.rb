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

require 'mongo/operation/commands/users_info/result'

module Mongo
  module Operation

    # A MongoDB operation to get info of a particular user in a database.
    #
    # @example Create the users info operation.
    #   Read::UsersInfo.new(:name => 'emily', :db_name => 'test-db')
    #
    # Initialization:
    #   param [ Hash ] spec The specifications for the users info operation.
    #
    #   option spec :user_name [ String ] The name of the user.
    #   option spec :db_name [ String ] The name of the database where the user exists.
    #   option spec :options [ Hash ] Options for the operation.
    #
    # @since 2.1.0
    class UsersInfo
      include Executable
      include Specifiable
      include Limited

      private

      def selector
        { :usersInfo => user_name }
      end

      def query_coll
        Database::COMMAND
      end

      def message(context)
        Protocol::Query.new(db_name, query_coll, selector, options)
      end
    end
  end
end

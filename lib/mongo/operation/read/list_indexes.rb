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

require 'mongo/operation/list_indexes/result'

module Mongo
  module Operation
    module Read

      # A MongoDB listIndexes command operation.
      #
      # @example Create the listIndexes command operation.
      #   Mongo::Operation::Read::ListIndexes.new({ db_name: 'test', coll_name: 'example' })
      #
      # @note A command is actually a query on the virtual '$cmd' collection.
      #
      # @param [ Hash ] spec The specifications for the command.
      #
      # @option spec :coll_name [ Hash ] The name of the collection whose index
      #   info is requested.
      # @option spec :db_name [ String ] The name of the database on which
      #   the command should be executed.
      # @option spec :options [ Hash ] Options for the command.
      #
      # @since 2.0.0
      class ListIndexes
        include Specifiable
        include Limited

        # Execute the listIndexes command operation.
        #
        # @example Execute the operation.
        #   operation.execute(context)
        #
        # @params [ Mongo::Server::Context ] The context for this operation.
        #
        # @return [ Result ] The operation result.
        #
        # @since 2.0.0
        def execute(context)
          unless context.primary? || context.standalone?
            raise Exception, "Must use primary server"
          end
          execute_message(context)
        end

        private

        def execute_message(context)
          context.with_connection do |connection|
            Result.new(connection.dispatch([ message ]))
          end
        end

        def message
          sel = (selector || {}).merge(listIndexes: coll_name)
          Protocol::Query.new(db_name, Database::COMMAND, sel, options)
        end
      end
    end
  end
end



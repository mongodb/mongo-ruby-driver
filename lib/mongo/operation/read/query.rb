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

module Mongo
  module Operation
    module Read

      # A MongoDB query operation.
      #
      # @example Create the query operation.
      #   Read::Query.new({
      #     :selector => { :foo => 1 },
      #     :db_name => 'test-db',
      #     :coll_name => 'test-coll',
      #     :options => { :limit => 2 }
      #   })
      #
      # @param [ Hash ] spec The specifications for the query.
      #
      # @option spec :selector [ Hash ] The query selector.
      # @option spec :db_name [ String ] The name of the database on which
      #   the query should be run.
      # @option spec :coll_name [ String ] The name of the collection on which
      #   the query should be run.
      # @option spec :options [ Hash ] Options for the query.
      #
      # @since 2.0.0
      class Query
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
          execute_message(context)
        end

        private

        def execute_message(context)
          context.with_connection do |connection|
            Result.new(connection.dispatch([ message(context) ]))
          end
        end

        def message(context)
          sel = (context.mongos? && read_pref = read.to_mongos) ?
                  selector.merge(:$readPreference => read_pref) : selector
          opts = read.slave_ok? || context.standalone? ?
                   options.merge(flags: [:slave_ok]) : options
          Protocol::Query.new(db_name, coll_name, sel, opts)
        end
      end
    end
  end
end

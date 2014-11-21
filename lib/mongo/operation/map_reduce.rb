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

require 'mongo/operation/map_reduce/result'

module Mongo
  module Operation

    # A MongoDB map reduce operation.
    #
    # @note A map/reduce operation can behave like a read and
    #   return a result set, or can behave like a write operation and
    #   output results to a user-specified collection.
    #
    # @example Create the map/reduce operation.
    #   MapReduce.new({
    #     :selector => {
    #       :mapreduce => 'test_coll',
    #       :map => '',
    #       :reduce => ''
    #     },
    #     :db_name  => 'test_db'
    #   })
    #
    # @param [ Hash ] spec The specifications for the operation.
    #
    # @option spec :selector [ Hash ] The map reduce selector.
    # @option spec :db_name [ String ] The name of the database on which
    #   the operation should be executed.
    # @option spec :options [ Hash ] Options for the map reduce command.
    #
    # @since 2.0.0
    class MapReduce
      include Executable
      include Specifiable
      include Limited

      # Execute the map/reduce operation.
      #
      # @example Execute the operation.
      #   operation.execute(context)
      #
      # @params [ Mongo::Server::Context ] The context for this operation.
      #
      # @return [ Result ] The operation response, if there is one.
      #
      # @since 2.0.0
      def execute(context)
        # @todo: Should we respect tag sets and options here?
        if context.secondary? && !secondary_ok?
          warn "Database command '#{selector.keys.first}' rerouted to primary server"
          context = Mongo::ServerPreference.get(:mode => :primary).server.context
        end
        execute_message(context)
      end

      private

      def execute_message(context)
        log(:debug, 'MONGODB | MAP/REDUCE', [ message ]) do |messages|
          context.with_connection do |connection|
            Result.new(connection.dispatch(messages)).validate!
          end
        end
      end

      # Whether this operation can be executed on a replica set secondary server.
      # The map reduce operation may not be executed on a secondary if the user has specified
      # an output collection to which the results will be written.
      #
      # @return [ true, false ] Whether the operation can be executed on a secondary.
      #
      # @since 2.0.0
      def secondary_ok?
        selector[:out] == 'inline'
      end

      def message
        Protocol::Query.new(db_name, Database::COMMAND, selector, options)
      end
    end
  end
end

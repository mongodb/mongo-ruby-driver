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

require 'mongo/operation/aggregate/result'

module Mongo
  module Operation

    # A MongoDB aggregate operation.
    #
    # @note An aggregate operation can behave like a read and return a 
    #   result set, or can behave like a write operation and
    #   output results to a user-specified collection.
    #
    # @example Create the aggregate operation.
    #   Aggregate.new({
    #     :selector => {
    #       :aggregate => 'test_coll', :pipeline => [{ '$out' => 'test-out' }]
    #     },
    #     :db_name => 'test_db'
    #   })
    #
    # @param [ Hash ] spec The specifications for the operation.
    #
    # @option spec :selector [ Hash ] The aggregate selector.
    # @option spec :db_name [ String ] The name of the database on which
    #   the operation should be executed.
    # @option spec :options [ Hash ] Options for the aggregate command.
    #
    # @since 2.0.0
    class Aggregate
      include Executable
      include Specifiable
      include Limited

      # The need primary error message.
      #
      # @since 2.0.0
      ERROR_MESSAGE = "The pipeline contains the '$out' operator so the primary must be used.".freeze

      # Execute the operation.
      # The context gets a connection on which the operation
      # is sent in the block.
      # If the aggregation will be written to an output collection and the
      # server is not primary, the operation will be rerouted to the primary
      # with a warning.
      #
      # @param [ Server::Context ] context The context for this operation.
      #
      # @return [ Result ] The operation response, if there is one.
      #
      # @since 2.0.0
      def execute(context)
        unless context.standalone? || context.primary? || secondary_ok?
          raise Error::NeedPrimaryServer.new(ERROR_MESSAGE)
        end
        execute_message(context)
      end

      private

      def execute_message(context)
        context.with_connection do |connection|
          Result.new(connection.dispatch([ message(context) ])).validate!
        end
      end

      # Whether this operation can be executed on a replica set secondary server.
      # The aggregate operation may not be executed on a secondary if the user has specified
      # an output collection to which the results will be written.
      #
      # @return [ true, false ] Whether the operation can be executed on a secondary.
      #
      # @since 2.0.0
      def secondary_ok?
        selector[:pipeline].none? { |op| op.key?('$out') || op.key?(:$out) }
      end

      def filter(context)
        return selector if context.features.write_command_enabled?
        selector.reject{ |option, value| option.to_s == 'cursor' }
      end

      def message(context)
        Protocol::Query.new(db_name, Database::COMMAND, filter(context), options)
      end
    end
  end
end

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
    # Initialization:
    #   param [ Hash ] spec The specifications for the operation.
    #
    #   option spec :selector [ Hash ] The map reduce selector.
    #   option spec :db_name [ String ] The name of the database on which
    #     the operation should be executed.
    #   option spec :options [ Hash ] Options for the map reduce command.
    #
    # @since 2.0.0
    class MapReduce
      include Executable
      include Specifiable
      include Limited
      include ReadPreferrable

      # The error message for needing a primary.
      #
      # @since 2.0.
      ERROR_MESSAGE = "If 'out' is specified as a collection, the primary server must be used.".freeze

      # Execute the map/reduce operation.
      #
      # @example Execute the operation.
      #   operation.execute(context)
      #
      # @param [ Server::Context ] context The context for this operation.
      #
      # @return [ Result ] The operation response, if there is one.
      #
      # @since 2.0.0
      def execute(context)
        unless valid_context?(context)
          raise Error::NeedPrimaryServer.new(ERROR_MESSAGE)
        end
        execute_message(context)
      end

      private

      def valid_context?(context)
        context.standalone? || context.mongos? || context.primary? || secondary_ok?
      end

      def execute_message(context)
        context.with_connection do |connection|
          Result.new(connection.dispatch([ message(context) ])).validate!
        end
      end

      def secondary_ok?
        selector[:out].respond_to?(:keys) &&
          selector[:out].keys.first.to_s.downcase == 'inline'
      end

      def query_coll
        Database::COMMAND
      end
    end
  end
end

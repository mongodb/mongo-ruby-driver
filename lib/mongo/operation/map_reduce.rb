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

    # A MongoDB map reduce operation.
    # Note that a map reduce operation can behave like a read and
    # return a result set, or can behave like a write operation and
    # output results to a user-specified collection.
    #
    # @since 2.0.0
    class MapReduce
      include Executable

      # Check equality of two map reduce operations.
      #
      # @example Check operation equality.
      #   operation == other
      #
      # @param [ Object ] other The other operation.
      #
      # @return [ true, false ] Whether the objects are equal.
      #
      # @since 2.0.0
      def ==(other)
        # @todo: check db name and map, reduce explicitly
        spec[:selector] == other.spec[:selector]
      end
      alias_method :eql?, :==

      # Initialize a map reduce operation.
      #
      # @example
      #   include Mongo
      #   include Operation
      #   MapReduce.new({ :selector => { :mapreduce => 'test_coll',
      #                                  :map => '',
      #                                  :reduce => '' },
      #                   :db_name  => 'test_db' })
      #
      # @param [ Hash ] spec The specifications for the operation.
      #
      # @option spec :selector [ Hash ] The map reduce selector.
      # @option spec :db_name [ String ] The name of the database on which
      #   the operation should be executed.
      # @option spec :opts [ Hash ] Options for the map reduce command.
      #
      # @since 2.0.0
      def initialize(spec)
        @spec = spec
      end

      # Execute the operation.
      # The context gets a connection on which the operation
      # is sent in the block.
      # If the map reduce will be written to an output collection and the
      # server is not primary, the operation will be rerouted to the primary
      # with a warning.
      #
      # @params [ Mongo::Server::Context ] The context for this operation.
      #
      # @return [ Mongo::Response ] The operation response, if there is one.
      #
      # @since 2.0.0
      def execute(context)
        if context.server.secondary? && !secondary_ok?
          warn "Database command '#{selector.keys.first}' rerouted to primary server"
          # @todo: Should we respect tag sets and options here?
          context = Mongo::ServerPreference.get(:mode => :primary).server.context
        end
        context.with_connection do |connection|
          connection.dispatch([message])
        end
      end

      private

      # The selector for this map reduce command operation.
      #
      # @return [ Hash ] The selector describing this map reduce operation.
      #
      # @since 2.0.0
      def selector
        @spec[:selector]
      end

      # Whether this operation can be executed on a replica set secondary server.
      # The map reduce operation may not be executed on a secondary if the user has specified
      # an output collection to which the results will be written.
      #
      # @return [ true, false ] Whether the operation can be executed on a secondary.
      #
      # @since 2.0.0
      def secondary_ok?
        out = selector[:out] || selector['out']
        out.nil? || out == 'inline'
      end

      # The wire protocol message for this operation.
      #
      # @return [ Mongo::Protocol::Query ] Wire protocol message. 
      #
      # @since 2.0.0
      def message
        Protocol::Query.new(db_name, Database::COMMAND, selector, options)
      end
    end
  end
end

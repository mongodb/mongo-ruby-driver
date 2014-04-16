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

    # A MongoDB aggregate operation with context describing
    # what server or socket it should be sent to.
    # Note that a aggregate operation can behave like a read and
    # return a result set, or can behave like a write operation and
    # output results to a user-specified collection.
    #
    # @since 3.0.0
    class Aggregate
      include Executable

      # Check equality of two aggregate operations.
      #
      # @example Check operation equality.
      #   operation == other
      #
      # @param [ Object ] other The other operation.
      #
      # @return [ true, false ] Whether the objects are equal.
      #
      # @since 3.0.0
      def ==(other)
        # @todo: is this the actual comparison we want to do?
        spec[:selector][:aggregate] == other.spec[:selector][:aggregate] &&
            spec[:selector][:pipeline] == other.spec[:selector][:pipeline] &&
            context == other.context
      end
      alias_method :eql?, :==

      # Initialize an aggregate operation.
      #
      # @example Initialize a aggregate operation.
      #   include Mongo
      #   include Operation
      #   primary_pref = Mongo::ServerPreference.get(:primary)
      #   Aggregate.new({ :selector => { :aggregate => 'test_coll',
      #                                  :pipeline => [{ '$out' => 'test-out' }] },
      #                   :db_name  => 'test_db' },
      #                   :server_preference => primary_pref)
      #
      # @param [ Hash ] spec The specifications for the operation.
      # @param [ Hash ] context The context for executing this operation.
      #
      # @option spec :selector [ Hash ] The aggregate selector.
      # @option spec :db_name [ String ] The name of the database on which
      #   the operation should be executed.
      # @option spec :opts [ Hash ] Options for the aggregate command.
      #
      # @option context :server_preference [ Object ] The server preference for where
      #   the operation should be sent.
      # @option context :server [ Mongo::Server ] The server to use for the operation.
      #
      # @since 3.0.0
      def initialize(spec, context={})
        @spec   = spec

        @server_preference = context[:server_preference]
        @server            = context[:server]
      end

      private

      # The selector for this aggregate command operation.
      #
      # @return [ Hash ] The selector describing this aggregate operation.
      #
      # @since 3.0.0
      def selector
        @spec[:selector]
      end

      # Any options for this aggregate command operation.
      #
      # @return [ Hash ] The query options.
      #
      # @since 3.0.0
      def query_opts
        @spec[:query_opts] || {}
      end

      # The server preference for the operation.
      # Note that if the user has specified a server preference that is not primary
      # and has also specified an output collection to which the results will be written,
      # the operation will be rerouted to the primary with a warning.
      #
      # @return [ Object ] The server preference.
      #
      # @since 3.0.0
      def server_preference
        return @server_preference = Mongo::ServerPreference.get(:primary) unless @server_preference
        if @server_preference.name != :primary && !secondary_ok?
          warn "Database command '#{selector.keys.first}' rerouted to primary server"
          @server_preference = Mongo::ServerPreference.get(:primary)
        end
        @server_preference
      end

      # Whether this operation can be executed on a replica set secondary server.
      # The aggregate operation may not be executed on a secondary if the user has specified
      # an output collection to which the results will be written.
      #
      # @return [ true, false ] Whether the operation can be executed on a secondary.
      #
      # @since 3.0.0
      def secondary_ok?
        selector[:pipeline].none? { |op| op.key?('$out') || op.key?(:$out) }
      end

      # The wire protocol message for this write operation.
      #
      # @return [ Mongo::Protocol::Query ] Wire protocol message.
      #
      # @since 3.0.0
      def message
        Mongo::Protocol::Query.new(db_name, Mongo::Operation::COMMAND_COLLECTION_NAME,
                                   selector, query_opts)
      end
    end
  end
end


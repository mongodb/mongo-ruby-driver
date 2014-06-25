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

    # A MongoDB command operation.
    # Note that a command is actually a query on the virtual '$cmd' collection.
    #
    # @since 3.0.0
    class Command
      include Executable

      # In general, commands must always be sent to a primary server.
      # There are some exceptions; the following commands may be sent
      # to secondaries.
      #
      SECONDARY_OK_COMMANDS = [
          'group',
          'aggregate',
          'collstats',
          'dbstats',
          'count',
          'distinct',
          'geonear',
          'geosearch',
          'geowalk',
          'mapreduce',
          'replsetgetstatus',
          'ismaster',
          'parallelcollectionscan'
      ].freeze

      # Initialize the command operation.
      #
      # @example
      #   Mongo::Operation::Command.new({ :selector => { :isMaster => 1 },
      #                                   :db_name => 'admin' })
      #
      # @param [ Hash ] spec The specifications for the command.
      #
      # @option spec :selector [ Hash ] The command selector.
      # @option spec :db_name [ String ] The name of the database on which
      #   the command should be executed.
      # @option spec :opts [ Hash ] Options for the command.
      #
      # @since 3.0.0
      def initialize(spec)
        @spec = spec
      end

      # Execute the operation.
      # The context gets a connection on which the operation
      # is sent in the block.
      # If the aggregation will be written to an output collection and the
      # server is not primary, the operation will be rerouted to the primary
      # with a warning.
      #
      # @params [ Mongo::Server::Context ] The context for this operation.
      #
      # @return [ Mongo::Response ] The operation response, if there is one.
      #
      # @since 3.0.0
      def execute(context)
        if context.server.secondary? && !secondary_ok?
          warn "Database command '#{selector.keys.first}' rerouted to primary server"
          context = Mongo::ServerPreference.get(:primary).server.context
        end
        context.with_connection do |connection|
          connection.dispatch([message])
        end
      end

      private

      # The database name on which the command will be executed..
      #
      # @return [ String ] The database name. 
      #
      # @since 3.0.0
      def db_name
        @spec[:db_name]
      end

      # The selector for the command.
      # Note that a command is actually a query on the virtual '$cmd' collection.
      #
      # @return [ Hash ] The command selector. 
      #
      # @since 3.0.0
      def selector
        @spec[:selector]
      end

      # Options for this command.
      #
      # @return [ Hash ] Command options.
      #
      # @since 3.0.0
      def opts
        @spec[:opts]
      end

      # Whether it is ok for this command to be executed on a secondary.
      #
      # @return [ true, false ] If this command can be executed on a secondary.
      #
      # @since 3.0.0
      def secondary_ok?
        command = selector.keys.first.to_s.downcase
        SECONDARY_OK_COMMANDS.include?(command)
      end

      # The wire protocol message for this operation.
      #
      # @return [ Mongo::Protocol::Query ] Wire protocol message. 
      #
      # @since 3.0.0
      def message
        Mongo::Protocol::Query.new(db_name, Mongo::Operation::COMMAND_COLLECTION_NAME,
                                   selector, opts)
      end
    end
  end
end



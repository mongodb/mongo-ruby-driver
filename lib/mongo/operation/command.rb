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

    # A MongoDB command operation.
    #
    # @example Create the command operation.
    #   Mongo::Operation::Command.new({ :selector => { :isMaster => 1 } })
    #
    # @note A command is actually a query on the virtual '$cmd' collection.
    #
    # @param [ Hash ] spec The specifications for the command.
    #
    # @option spec :selector [ Hash ] The command selector.
    # @option spec :db_name [ String ] The name of the database on which
    #   the command should be executed.
    # @option spec :options [ Hash ] Options for the command.
    #
    # @since 2.0.0
    class Command
      include Executable
      include Specifiable
      include Limited

      # In general, commands must always be sent to a primary server.
      # There are some exceptions; the following commands may be sent
      # to secondaries.
      #
      # @since 2.0.0
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
        'parallelcollectionscan',
        'text'
      ].freeze

      # Execute the command operation.
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
        # @todo: Should we respect tag sets and options here?
        if context.server.secondary? && !secondary_ok?
          warn "Database command '#{selector.keys.first}' rerouted to primary server"
          context = Mongo::ReadPreference.get(:mode => :primary).server.context
        end
        execute_message(context)
      end

      private

      def execute_message(context)
        context.with_connection do |connection|
          Result.new(connection.dispatch([ message ])).validate!
        end
      end

      def secondary_ok?
        command = selector.keys.first.to_s.downcase
        SECONDARY_OK_COMMANDS.include?(command)
      end

      def message
        Protocol::Query.new(db_name, Database::COMMAND, selector, options)
      end
    end
  end
end



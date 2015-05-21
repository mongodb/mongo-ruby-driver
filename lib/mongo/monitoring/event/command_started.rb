# Copyright (C) 2015 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

module Mongo
  module Monitoring
    module Event

      # Event that is fired when a command operation starts.
      #
      # @since 2.1.0
      class CommandStarted

        # @return [ BSON::Document ] arguments The command arguments.
        attr_reader :arguments

        # @return [ String ] name The name of the command.
        attr_reader :name

        # @return [ String ] database The name of the database.
        attr_reader :database

        # @return [ String ] connection The server address.
        attr_reader :connection

        # Create the new event.
        #
        # @example Create the event.
        #   CommandCompleted.new('createIndexes', 'users', '127.0.0.1:27017', { name: 1 })
        #
        # @param [ String ] name The name of the command.
        # @param [ String ] database The database name.
        # @param [ String ] connection The server connected to.
        # @param [ BSON::Document ] arguments The command arguments.
        #
        # @since 2.1.0
        def initialize(name, database, connection, arguments)
          @name = name
          @database = database
          @connection = connection
          @arguments = arguments
        end
      end
    end
  end
end

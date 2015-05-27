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
  class Monitoring
    module Event

      # Event that is fired when a command operation fails.
      #
      # @since 2.1.0
      class CommandFailed

        # @return [ String ] name The name of the command.
        attr_reader :name

        # @return [ String ] database The name of the database.
        attr_reader :database

        # @return [ String ] connection The server address.
        attr_reader :connection

        # @return [ String ] message The error message.
        attr_reader :message

        # @return [ Float ] duration The duration of the event.
        attr_reader :duration

        # Create the new event.
        #
        # @example Create the event.
        #   CommandFailed.new('127.0.0.1:27017', 'Authorization failed.', 1.2)
        #
        # @param [ String ] connection The server connected to.
        # @param [ String ] message The error message.
        # @param [ Float ] duration The event duration, in seconds.
        #
        # @since 2.1.0
        def initialize(name, database, connection, message, duration)
          @name = name
          @database = database
          @connection = connection
          @duration = duration
          @message = message
        end
      end
    end
  end
end

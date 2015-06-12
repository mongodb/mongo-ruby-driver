
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
    module Write

      # A MongoDB create user operation.
      #
      # @example Initialize the operation.
      #   Write::CreateUser.new(:db_name => 'test', :user => user)
      #
      # Initialization:
      #   param [ Hash ] spec The specifications for the create.
      #
      #   option spec :user [ Auth::User ] The user to create.
      #   option spec :db_name [ String ] The name of the database.
      #
      # @since 2.0.0
      class CreateUser
        include Executable
        include Specifiable

        # Execute the operation.
        #
        # @note Creating users behaves different on 2.7+, 2.6.x, and
        #   2.4- so we need to break this out into separate operations.
        #
        # @example Execute the operation.
        #   operation.execute(context)
        #
        # @param [ Mongo::Server::Context ] context The context for this operation.
        #
        # @return [ Result ] The operation result.
        #
        # @since 2.0.0
        def execute(context)
          if context.features.write_command_enabled?
            execute_write_command(context)
          else
            execute_message(context)
          end
        end

        private

        def execute_write_command(context)
          Result.new(Command::CreateUser.new(spec).execute(context)).validate!
        end

        def execute_message(context)
          context.with_connection do |connection|
            Result.new(connection.dispatch([ message, gle ].compact)).validate!
          end
        end

        def message
          user_spec = { user: user.name }.merge(user.spec)
          Protocol::Insert.new(db_name, Auth::User::COLLECTION, [ user_spec ])
        end
      end
    end
  end
end

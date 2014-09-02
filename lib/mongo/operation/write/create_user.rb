
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

require 'mongo/operation/write/create_user/response'

module Mongo
  module Operation
    module Write

      # A MongoDB create user operation.
      #
      # @since 2.0.0
      class CreateUser
        include Executable

        # Initialize the create user operation.
        #
        # @example Initialize the operation.
        #   Write::CreateUser.new(:db_name => 'test', :user => user)
        #
        # @param [ Hash ] spec The specifications for the create.
        #
        # @option spec :user [ Auth::User ] The user to create.
        # @option spec :db_name [ String ] The name of the database.
        #
        # @since 2.0.0
        def initialize(spec)
          @spec = spec
        end

        # Execute the operation. Creating users behaves different on 2.7+,
        # 2.6.x, and 2.4- so we need to break this out into separate
        # operations.
        #
        # @example Execute the operation.
        #   operation.execute(context)
        #
        # @param [ Mongo::Server::Context ] The context for this operation.
        #
        # @return [ Mongo::Response ] The operation response, if there is one.
        #
        # @since 2.0.0
        def execute(context)
          Response.new(
            if context.write_command_enabled?
              Command::CreateUser.new(spec).execute(context)
            else
              context.with_connection do |connection|
                connection.dispatch([ message, gle ].compact)
              end
            end
          ).verify!
        end

        private

        def message
          user_spec = { user: user.name }.merge(user.spec)
          Protocol::Insert.new(db_name, View::User::COLLECTION, [ user_spec ])
        end
      end
    end
  end
end

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

require 'mongo/operation/write/remove_user/response'

module Mongo
  module Operation
    module Write

      # A MongoDB remove user operation.
      #
      # @since 2.0.0
      class RemoveUser
        include Executable

        # Initialize the remove user operation.
        #
        # @example Initialize the operation.
        #   Write::RemoveUser.new(:db_name => 'test', :name => name)
        #
        # @param [ Hash ] spec The specifications for the remove.
        #
        # @option spec :name [ String ] The user name.
        # @option spec :db_name [ String ] The name of the database.
        #
        # @since 2.0.0
        def initialize(spec)
          @spec = spec
        end

        # Execute the operation.
        # If the server has version < 2.5.5, an insert operation is sent.
        # If the server version is >= 2.5.5, an insert write command operation is created
        # and sent instead.
        #
        # @params [ Mongo::Server::Context ] The context for this operation.
        #
        # @return [ Mongo::Response ] The operation response, if there is one.
        #
        # @since 2.0.0
        def execute(context)
          Response.new(
            if context.write_command_enabled?
              Command::RemoveUser.new(spec).execute(context)
            else
              context.with_connection do |connection|
                connection.dispatch([ message, gle ].compact)
              end
            end
          ).verify!
        end

        private

        def message
          Protocol::Delete.new(db_name, View::User::COLLECTION, { user: @spec[:name] })
        end
      end
    end
  end
end

# Copyright (C) 2014-2019 MongoDB, Inc.
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
  module Auth
    class User

      # Defines behavior for user related operation on databases.
      #
      # @since 2.0.0
      class View
        extend Forwardable

        # @return [ Database ] database The view's database.
        attr_reader :database

        def_delegators :database, :cluster, :read_preference, :client
        def_delegators :cluster, :next_primary

        # Create a new user in the database.
        #
        # @example Create a new read/write user.
        #   view.create('user', password: 'password', roles: [ 'readWrite' ])
        #
        # @param [ Auth::User, String ] user_or_name The user object or user name.
        # @param [ Hash ] options The user options.
        #
        # @option options [ Session ] :session The session to use for the operation.
        #
        # @return [ Result ] The command response.
        #
        # @since 2.0.0
        def create(user_or_name, options = {})
          user = generate(user_or_name, options)
          client.send(:with_session, options) do |session|
            Operation::CreateUser.new(
              user: user,
              db_name: database.name,
              session: session
            ).execute(next_primary(nil, session))
          end
        end

        # Initialize the new user view.
        #
        # @example Initialize the user view.
        #   View::User.new(database)
        #
        # @param [ Mongo::Database ] database The database the view is for.
        #
        # @since 2.0.0
        def initialize(database)
          @database = database
        end

        # Remove a user from the database.
        #
        # @example Remove the user from the database.
        #   view.remove('user')
        #
        # @param [ String ] name The user name.
        # @param [ Hash ] options The options for the remove operation.
        #
        # @option options [ Session ] :session The session to use for the operation.
        #
        # @return [ Result ] The command response.
        #
        # @since 2.0.0
        def remove(name, options = {})
          client.send(:with_session, options) do |session|
            Operation::RemoveUser.new(
              user_name: name,
              db_name: database.name,
              session: session
            ).execute(next_primary(nil, session))
          end
        end

        # Update a user in the database.
        #
        # @example Update a user.
        #   view.update('name', password: 'testpwd')
        #
        # @param [ Auth::User, String ] user_or_name The user object or user name.
        # @param [ Hash ] options The user options.
        #
        # @option options [ Session ] :session The session to use for the operation.
        #
        # @return [ Result ] The response.
        #
        # @since 2.0.0
        def update(user_or_name, options = {})
          client.send(:with_session, options) do |session|
            user = generate(user_or_name, options)
            Operation::UpdateUser.new(
              user: user,
              db_name: database.name,
              session: session
            ).execute(next_primary(nil, session))
          end
        end

        # Get info for a particular user in the database.
        #
        # @example Get a particular user's info.
        #   view.info('emily')
        #
        # @param [ String ] name The user name.
        # @param [ Hash ] options The options for the info operation.
        #
        # @option options [ Session ] :session The session to use for the operation.
        #
        # @return [ Hash ] A document containing information on a particular user.
        #
        # @since 2.1.0
        def info(name, options = {})
          user_query(name, options).documents
        end

        private

        def user_query(name, options = {})
          client.send(:with_session, options) do |session|
            Operation::UsersInfo.new(
              user_name: name,
              db_name: database.name,
              session: session
            ).execute(next_primary(nil, session))
          end
        end

        def generate(user, options)
          user.is_a?(String) ? Auth::User.new({ user: user }.merge(options)) : user
        end
      end
    end
  end
end

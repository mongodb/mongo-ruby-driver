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
  module View

    # Defines behaviour for user related operation on databases.
    #
    # @since 2.0.0
    class User
      extend Forwardable

      # The users collection for the database.
      #
      # @since 2.0.0
      COLLECTION = 'system.users'.freeze

      # @return [ Database ] database The view's database.
      attr_reader :database

      # Create a new user in the database.
      #
      # @example Create a new read/write user.
      #   view.create('user', 'password', roles: [ 'readWrite' ])
      #
      # @param [ String ] name The user name.
      # @param [ String ] password The user password.
      # @param [ Hash ] options The user options.
      #
      # @return [ Response ] The command response.
      #
      # @since 2.0.0
      def create(name, password, options = {})
        user = Auth::User.new({ user: name, password: password }.merge(options))
        database.command(createUser: user.name, pwd: user.hashed_password, roles: user.roles)
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
      #
      # @return [ Response ] The command response.
      #
      # @since 2.0.0
      def remove(name)
        database.command(dropUser: name)
      end
    end
  end
end

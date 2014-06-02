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
  module Auth

    # Common functionality for authenticators.
    #
    # @since 2.0.0
    module Authenticatable

      # @return [ String ] db_name The current database name.
      attr_reader :db_name
      # @return [ String ] source The source database..
      attr_reader :source

      # Instantiate a new authenticator.
      #
      # @param [ String ] db_name The current database name.
      # @param [ String ] username The current username.
      # @param [ Hash ] opts Options for this authenticator.
      #
      # @options opts [ String ] :password The user's password.
      # @options opts [ String ] :source The source database, if different from the
      #   current database.
      # @options opts [ String ] :gssapi_service_name For GSSAPI authentication
      #   only.
      # @options opts [ true, false ] :canonicalize_host_name For GSSAPI 
      #   authentication only.
      #
      # @since 2.0.0
      def initialize(db_name, username, opts={})
        @db_name  = db_name
        @username = username
        @source   = opts[:source] || db_name || 'admin'
        @opts     = opts
        validate_credentials
      end

      # Log into the given server on this authenticator.
      #
      # @param [ Mongo::Server ] server The server on which to authenticate.
      #
      # @since 2.0.0
      def login(server)
        server.dispatch(login_message)
      end

      # Log out of the given server with these credentials.
      #
      # @note: this will log out any credentials with the same db_name on this server.
      #
      # @since 2.0.0
      def logout(server)
        server.dispatch(logout_message)
      end

      private

      # Generate a logout message.
      #
      # @return [ Mongo::Protocol::Query ] logout message.
      #
      # @since 2.0.0
      def logout_message
        Mongo::Protocol::Query.new(db_name,
                                   Mongo::Operation::COMMAND_COLLECTION_NAME,
                                   { :logout => 1 })
      end

      # Generate a login message.
      #
      # @return [ Mongo::Protocol::Query ] login message.
      #
      # @note: authenticatable modules should implement this, or override the login
      #   method.
      #
      # @since 2.0.0
      def login_message
        nil
      end

      # Validate credentials for this authenticator.
      #
      # @note: authenticatable modules should override this if validation is needed.
      #
      # @since 2.0.0
      def validate_credentials
        nil
      end
    end
  end
end

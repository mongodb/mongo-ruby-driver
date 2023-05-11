# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2014-2020 MongoDB Inc.
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

    # Defines behavior for X.509 authentication.
    #
    # @since 2.0.0
    # @api private
    class X509 < Base

      # The authentication mechanism string.
      #
      # @since 2.0.0
      MECHANISM = 'MONGODB-X509'.freeze

      # Initializes the X.509 authenticator.
      #
      # @param [ Auth::User ] user The user to authenticate.
      # @param [ Mongo::Connection ] connection The connection to authenticate over.
      def initialize(user, connection, **opts)
        # The only valid database for X.509 authentication is $external.
        if user.auth_source != '$external'
          user_name_msg = if user.name
            " #{user.name}"
          else
            ''
          end
          raise Auth::InvalidConfiguration, "User#{user_name_msg} specifies auth source '#{user.auth_source}', but the only valid auth source for X.509 is '$external'"
        end

        super
      end

      # Log the user in on the current connection.
      #
      # @return [ BSON::Document ] The document of the authentication response.
      def login
        converse_1_step(connection, conversation)
      end
    end
  end
end

require 'mongo/auth/x509/conversation'

# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2024 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

module Mongo
  module Auth

    # Defines behavior for OIDC authentication.
    #
    # @api private
    class Oidc < Base
      attr_reader :speculative_auth_result, :cache, :machine_workflow

      # The authentication mechanism string.
      #
      # @since 2.20.0
      MECHANISM = 'MONGODB-OIDC'.freeze

      # Initializes the OIDC authenticator.
      #
      # @param [ Auth::User ] user The user to authenticate.
      # @param [ Mongo::Connection ] connection The connection to authenticate over.
      #
      # @option opts [ BSON::Document | nil ] speculative_auth_result The
      #   value of speculativeAuthenticate field of hello response of
      #   the handshake on the specified connection.
      def initialize(user, connection, **opts)
        super
        @cache = TokenCache.new
        @speculative_auth_result = opts[:speculative_auth_result]
        @machine_workflow = MachineWorkflow::new(
          auth_mech_properties: user.auth_mech_properties,
          username: user.name
        )
      end

      # Log the user in on the current connection.
      #
      # @return [ BSON::Document ] The document of the authentication response.
      def login
        execute_workflow(connection: connection, conversation: conversation)
      end

      private

      def execute_workflow(connection:, conversation:)
        # If there is a cached access token, try to authenticate with it. If
        # authentication fails with an Authentication error (18),
        # invalidate the access token, fetch a new access token, and try
        # to authenticate again.
        # If the server fails for any other reason, do not clear the cache.
        if cache.access_token?
          token = cache.access_token
          msg = conversation.start(connection: connection, token: token)
          begin
            dispatch_msg(connection, conversation, msg)
          rescue AuthError => error
            cache.invalidate(token: token)
            execute_workflow(connection: connection, conversation: conversation)
          end
        end
        # This is the normal flow when no token is in the cache. Execute the
        # machine callback to get the token, put it in the caches, and then
        # send the saslStart to the server.
        token = machine_workflow.execute
        if token.nil? || !token[:access_token]
          raise Error::OidcError,
            "OIDC machine workflows must return a valid response with an access token but #{token} was returned"
        end
        cache.access_token = token[:access_token]
        connection.access_token = token[:access_token]
        msg = conversation.start(connection: connection, token: token[:access_token])
        dispatch_msg(connection, conversation, msg)
      end
    end
  end
end

require 'mongo/auth/oidc/conversation'
require 'mongo/auth/oidc/machine_workflow'
require 'mongo/auth/oidc/token_cache'

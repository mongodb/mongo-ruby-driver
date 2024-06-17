# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2024 MongoDB Inc.
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
    class Oidc
      # Defines behaviour around a single OIDC conversation between the
      # client and the server.
      #
      # @api private
      class Conversation < ConversationBase
        # The base client message.
        START_MESSAGE = { saslStart: 1, mechanism: Oidc::MECHANISM }.freeze

        # Create the new conversation.
        #
        # @example Create the new conversation.
        #   Conversation.new(user, 'test.example.com')
        #
        # @param [ Auth::User ] user The user to converse about.
        # @param [ Mongo::Connection ] connection The connection to
        #   authenticate over.
        #
        # @since 2.20.0
        def initialize(user, connection, **opts)
          super
        end

        # OIDC machine workflow is always a saslStart with the payload being
        # the serialized jwt token.
        #
        # @param [ String ] token The access token.
        #
        # @return [ Hash ] The start document.
        def client_start_document(token:)
          START_MESSAGE.merge(payload: finish_payload(token: token))
        end

        # Gets the serialized jwt payload for the token.
        #
        # @param [ String ] token The access token.
        #
        # @return [ BSON::Binary ] The serialized payload.
        def finish_payload(token:)
          payload = { jwt: token }.to_bson.to_s
          BSON::Binary.new(payload)
        end

        # Start the OIDC conversation. This returns the first message that
        # needs to be sent to the server.
        #
        # @param [ Server::Connection ] connection The connection being authenticated.
        #
        # @return [ Protocol::Message ] The first OIDC conversation message.
        def start(connection:, token:)
          selector = client_start_document(token: token)
          build_message(connection, '$external', selector)
        end
      end
    end
  end
end

# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2015-2020 MongoDB Inc.
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
    class Gssapi

      # Defines behaviour around a single Kerberos conversation between the
      # client and the server.
      #
      # @api private
      class Conversation < SaslConversationBase

        # The base client first message.
        START_MESSAGE = { saslStart: 1, autoAuthorize: 1 }.freeze

        # The base client continue message.
        CONTINUE_MESSAGE = { saslContinue: 1 }.freeze

        # Create the new conversation.
        #
        # @example Create the new conversation.
        #   Conversation.new(user, 'test.example.com')
        #
        # @param [ Auth::User ] user The user to converse about.
        # @param [ Mongo::Connection ] connection The connection to
        #   authenticate over.
        #
        # @since 2.0.0
        def initialize(user, connection, **opts)
          super
          host = connection.address.host
          unless defined?(Mongo::GssapiNative)
            require 'mongo_kerberos'
          end
          @authenticator = Mongo::GssapiNative::Authenticator.new(
            user.name,
            host,
            user.auth_mech_properties[:service_name] || 'mongodb',
            user.auth_mech_properties[:canonicalize_host_name] || false,
          )
        end

        # @return [ Authenticator ] authenticator The native SASL authenticator.
        attr_reader :authenticator

        # Get the id of the conversation.
        #
        # @return [ Integer ] The conversation id.
        attr_reader :id

        def client_first_document
          start_token = authenticator.initialize_challenge
          START_MESSAGE.merge(mechanism: Gssapi::MECHANISM, payload: start_token)
        end

        # Continue the conversation.
        #
        # @param [ BSON::Document ] reply_document The reply document of the
        #   previous message.
        #
        # @return [ Protocol::Message ] The next query to execute.
        def continue(reply_document, connection)
          @id = reply_document['conversationId']
          payload = reply_document['payload']

          continue_token = authenticator.evaluate_challenge(payload)
          selector = CONTINUE_MESSAGE.merge(payload: continue_token, conversationId: id)
          build_message(connection, '$external', selector)
        end

        def process_continue_response(reply_document)
          payload = reply_document['payload']

          @continue_token = authenticator.evaluate_challenge(payload)
        end

        # @return [ Protocol::Message ] The next query to execute.
        def finalize(connection)
          selector = CONTINUE_MESSAGE.merge(payload: @continue_token, conversationId: id)
          build_message(connection, '$external', selector)
        end
      end
    end
  end
end

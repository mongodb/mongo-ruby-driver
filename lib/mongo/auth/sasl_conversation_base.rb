# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2020 MongoDB Inc.
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

    # Defines common behavior around SASL conversations between
    # the client and the server.
    #
    # @api private
    class SaslConversationBase < ConversationBase

      # The base client first message.
      CLIENT_FIRST_MESSAGE = { saslStart: 1, autoAuthorize: 1 }.freeze

      # The base client continue message.
      CLIENT_CONTINUE_MESSAGE = { saslContinue: 1 }.freeze

      # Start the SASL conversation. This returns the first message that
      # needs to be sent to the server.
      #
      # @param [ Server::Connection ] connection The connection being authenticated.
      #
      # @return [ Protocol::Message ] The first SASL conversation message.
      def start(connection)
        selector = client_first_document
        build_message(connection, user.auth_source, selector)
      end

      private

      # Gets the auth mechanism name for the conversation class.
      #
      # Example return: SCRAM-SHA-1.
      #
      # @return [ String ] Auth mechanism name.
      def auth_mechanism_name
        # self.class.name is e.g. Mongo::Auth::Scram256::Mechanism.
        # We need Mongo::Auth::Scram::MECHANISM.
        # Pull out the Scram256 part, get that class off of Auth,
        # then get the value of MECHANISM constant in Scram256.
        # With ActiveSupport, this method would be:
        # self.class.module_parent.const_get(:MECHANISM)
        parts = self.class.name.split('::')
        parts.pop
        Auth.const_get(parts.last).const_get(:MECHANISM)
      end

      def client_first_message_options
        nil
      end

      def client_first_document
        payload = client_first_payload
        if Lint.enabled?
          unless payload.is_a?(String)
            raise Error::LintError, "Payload must be a string but is a #{payload.class}: #{payload}"
          end
        end
        doc = CLIENT_FIRST_MESSAGE.merge(
          mechanism: auth_mechanism_name,
          payload: BSON::Binary.new(payload),
        )
        if options = client_first_message_options
          # Short SCRAM conversation,
          # https://jira.mongodb.org/browse/DRIVERS-707
          doc[:options] = options
        end
        doc
      end

      # Helper method to validate that server nonce starts with the client
      # nonce.
      #
      # Note that this class does not define the client_nonce or server_nonce
      # attributes - derived classes must do so.
      def validate_server_nonce!
        if client_nonce.nil? || client_nonce.empty?
          raise ArgumentError, 'Cannot validate server nonce when client nonce is nil or empty'
        end

        unless server_nonce.start_with?(client_nonce)
          raise Error::InvalidNonce.new(client_nonce, server_nonce)
        end
      end
    end
  end
end

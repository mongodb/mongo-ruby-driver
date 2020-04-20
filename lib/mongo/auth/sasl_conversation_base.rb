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
        payload = client_first_payload
        if Lint.enabled?
          unless payload.is_a?(String)
            raise Error::LintError, "Payload must be a string but is a #{payload.class}: #{payload}"
          end
        end
        selector = CLIENT_FIRST_MESSAGE.merge(
          mechanism: full_mechanism,
          payload: BSON::Binary.new(payload),
        )
        if options = client_first_message_options
          # Short SCRAM conversation,
          # https://jira.mongodb.org/browse/DRIVERS-707
          selector[:options] = options
        end
        if connection && connection.features.op_msg_enabled?
          selector[Protocol::Msg::DATABASE_IDENTIFIER] = user.auth_source
          cluster_time = connection.mongos? && connection.cluster_time
          selector[Operation::CLUSTER_TIME] = cluster_time if cluster_time
          Protocol::Msg.new([], {}, selector)
        else
          Protocol::Query.new(
            user.auth_source,
            Database::COMMAND,
            selector,
            limit: -1,
          )
        end
      end

      private

      def client_first_message_options
        nil
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

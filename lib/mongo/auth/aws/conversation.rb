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
    class Aws

      # Defines behavior around a single MONGODB-AWS conversation between the
      # client and server.
      #
      # @see https://github.com/mongodb/specifications/blob/master/source/auth/auth.rst#mongodb-aws
      #
      # @api private
      class Conversation < SaslConversationBase

        # Continue the AWS conversation. This sends the client final message
        # to the server after setting the reply from the previous server
        # communication.
        #
        # @param [ BSON::Document ] reply_document The reply document of the
        #   previous message.
        # @param [ Server::Connection ] connection The connection being
        #   authenticated.
        #
        # @return [ Protocol::Message ] The next message to send.
        def continue(reply_document, connection)
          @conversation_id = reply_document[:conversationId]
          payload = reply_document[:payload].data
          payload = BSON::Document.from_bson(BSON::ByteBuffer.new(payload))
          @server_nonce = payload[:s].data
          validate_server_nonce!
          @sts_host = payload[:h]
          unless (1..255).include?(@sts_host.bytesize)
            raise Error::InvalidServerAuthConfiguration, "STS host name length is not in 1..255 bytes range: #{@sts_host}"
          end

          selector = CLIENT_CONTINUE_MESSAGE.merge(
            payload: BSON::Binary.new(client_final_payload),
            conversationId: conversation_id,
          )
          build_message(connection, user.auth_source, selector)
        end

        private

        # @return [ String ] The server nonce.
        attr_reader :server_nonce

        # Get the id of the conversation.
        #
        # @return [ Integer ] The conversation id.
        attr_reader :conversation_id

        def client_first_data
          {
            r: BSON::Binary.new(client_nonce),
            p: 110,
          }
        end

        def client_first_payload
          client_first_data.to_bson.to_s
        end

        def wrap_data(data)
          BSON::Binary.new(data.to_bson.to_s)
        end

        def client_nonce
          @client_nonce ||= SecureRandom.random_bytes(32)
        end

        def client_final_payload
          credentials = CredentialsRetriever.new(user).credentials
          request = Request.new(
            access_key_id: credentials.access_key_id,
            secret_access_key: credentials.secret_access_key,
            session_token: credentials.session_token,
            host: @sts_host,
            server_nonce: server_nonce,
          )

          # Uncomment this line to validate obtained credentials on the
          # client side prior to sending them to the server.
          # This generally produces informative diagnostics as to why
          # the credentials are not valid (e.g., they could be expired)
          # whereas the server normally does not elaborate on why
          # authentication failed (but the reason usually is logged into
          # the server logs).
          #
          # Note that credential validation requires that the client is
          # able to access AWS STS. If this is not permitted by firewall
          # rules, validation will fail but credentials may be perfectly OK
          # and the server may be able to authenticate using them just fine
          # (provided the server is allowed to communicate with STS).
          #request.validate!

          payload = {
            a: request.authorization,
            d: request.formatted_time,
          }
          if credentials.session_token
            payload[:t] = credentials.session_token
          end
          payload.to_bson.to_s
        end
      end
    end
  end
end

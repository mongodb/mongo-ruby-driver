# Copyright (C) 2014 MongoDB Inc.
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

    # Defines behaviour for SCRAM-SHA1 authentication.
    #
    # @since 2.0.0
    class SCRAM
      include Executable

      CONVERSATION_ID = 'conversationId'.freeze

      DONE = 'done'.freeze

      # The authentication mechinism string.
      #
      # @since 2.0.0
      MECHANISM = 'SCRAM-SHA-1'.freeze

      PAYLOAD = 'payload'.freeze

      # Log the user in on the given connection.
      #
      # @example Log the user in.
      #   user.login(connection)
      #
      # @param [ Mongo::Connection ] connection The connection to log into.
      #   on.
      #
      # @return [ Protocol::Reply ] The authentication response.
      #
      # @since 2.0.0
      def login(connection)
        token = user.nonce
        reply = connection.dispatch([ login_message(token) ]).documents[0]
        reply = connection.dispatch([ continue_message(reply, token) ]).documents[0]
        reply = connection.dispatch([ final_message(response) ]) until reply[DONE]
        reply
      end

      private

      def login_message(token)
        Protocol::Query.new(
          Auth::EXTERNAL,
          Database::COMMAND,
          { saslStart: 1, payload: token, mechanism: MECHANISM, authAuthorize: 1 },
          limit: -1
        )
      end

      def continue_message(reply, token)
        payload = continue_token(reply, response_nonce(reply, token))
        Protocol::Query.new(
          Auth::EXTERNAL,
          Database::COMMAND,
          { saslContinue: 1, payload: payload, conversationId: response[CONVERSATION_ID] },
          limit: -1
        )
      end

      def continue_token(reply, token)
        salt = reply[PAYLOAD]['s']
        iterations = reply[PAYLOAD]['i']
        # OpenSSL::PKCS5.pbkdf2_hmac(pass, salt, iterations, len, digest)
        response_token = "c=biws,r=#{nonce},p=#{user.salted_password(salt, iterations)}"
      end

      def final_message(response)
        Protocol::Query.new(
          Auth::EXTERNAL,
          Database::COMMAND,
          { saslContinue: 1, payload: "", conversationId: response[CONVERSATION_ID] },
          limit: -1
        )
      end

      def response_nonce(reply, token)
        nonce = reply[PAYLOAD]['r']
        raise Exeption, '' unless nonce.starts_with(token)
        nonce
      end
    end
  end
end

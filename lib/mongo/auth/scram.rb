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

    # Defines behavior for SCRAM authentication.
    #
    # @api private
    class Scram < Base

      # The authentication mechanism string.
      MECHANISM = 'SCRAM-SHA-1'.freeze

      # Log the user in on the given connection.
      #
      # @param [ Mongo::Connection ] connection The connection to log into.
      # @param [ String | nil ] speculative_auth_client_nonce The client
      #   nonce used in speculative auth on the specified connection that
      #   produced the specified speculative auth result.
      # @param [ BSON::Document | nil ] speculative_auth_result The
      #   value of speculativeAuthenticate field of ismaster response of
      #   the handshake on the specified connection.
      #
      # @return [ BSON::Document ] The document of the authentication response.
      #
      # @since 2.0.0
      def login(connection, speculative_auth_client_nonce: nil, speculative_auth_result: nil)
        @conversation = self.class.const_get(:Conversation).new(
          user, client_nonce: speculative_auth_client_nonce)
        converse_multi_step(connection, conversation,
          speculative_auth_result: speculative_auth_result,
        ).tap do
          unless conversation.server_verified?
            raise Error::MissingScramServerSignature
          end
        end
      end
    end
  end
end

require 'mongo/auth/scram/conversation'

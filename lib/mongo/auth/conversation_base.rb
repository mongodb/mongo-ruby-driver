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

    # Defines common behavior around authentication conversations between
    # the client and the server.
    #
    # @api private
    class ConversationBase

      # Create the new conversation.
      #
      # @param [ Auth::User ] user The user to authenticate.
      # @param [ Mongo::Connection ] connection The connection to authenticate
      #   over.
      def initialize(user, connection, **opts)
        @user = user
        @connection = connection
      end

      # @return [ Auth::User ] user The user for the conversation.
      attr_reader :user

      # @return [ Mongo::Connection ] The connection to authenticate over.
      attr_reader :connection

      # Returns the hash to provide to the server in the handshake
      # as value of the speculativeAuthenticate key.
      #
      # If the auth mechanism does not support speculative authentication,
      # this method returns nil.
      #
      # @return [ Hash | nil ] Speculative authentication document.
      def speculative_auth_document
        nil
      end
    end
  end
end

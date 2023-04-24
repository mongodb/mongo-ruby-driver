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

      # @return [ Protocol::Message ] The message to send.
      def build_message(connection, auth_source, selector)
        if connection && connection.features.op_msg_enabled?
          selector = selector.dup
          selector[Protocol::Msg::DATABASE_IDENTIFIER] = auth_source
          cluster_time = connection.mongos? && connection.cluster_time
          if cluster_time
            selector[Operation::CLUSTER_TIME] = cluster_time
          end
          Protocol::Msg.new([], {}, selector)
        else
          Protocol::Query.new(
            auth_source,
            Database::COMMAND,
            selector,
            limit: -1,
          )
        end
      end

      def validate_external_auth_source
        if user.auth_source != '$external'
          user_name_msg = if user.name
            " #{user.name}"
          else
            ''
          end
          mechanism = user.mechanism
          raise Auth::InvalidConfiguration, "User#{user_name_msg} specifies auth source '#{user.auth_source}', but the only valid auth source for #{mechanism} is '$external'"
        end
      end
    end
  end
end

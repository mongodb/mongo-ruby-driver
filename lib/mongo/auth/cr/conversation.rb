# frozen_string_literal: true
# rubocop:todo all

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
    class CR

      # Defines behavior around a single MONGODB-CR conversation between the
      # client and server.
      #
      # @since 2.0.0
      # @deprecated MONGODB-CR authentication mechanism is deprecated
      #   as of MongoDB 3.6. Support for it in the Ruby driver will be
      #   removed in driver version 3.0. Please use SCRAM instead.
      # @api private
      class Conversation < ConversationBase

        # The login message base.
        #
        # @since 2.0.0
        LOGIN = { authenticate: 1 }.freeze

        # @return [ String ] database The database to authenticate against.
        attr_reader :database

        # @return [ String ] nonce The initial auth nonce.
        attr_reader :nonce

        # Start the CR conversation. This returns the first message that
        # needs to be sent to the server.
        #
        # @param [ Server::Connection ] connection The connection being
        #   authenticated.
        #
        # @return [ Protocol::Message ] The first CR conversation message.
        #
        # @since 2.0.0
        def start(connection)
          selector = Auth::GET_NONCE
          build_message(connection, user.auth_source, selector)
        end

        # Continue the CR conversation. This sends the client final message
        # to the server after setting the reply from the previous server
        # communication.
        #
        # @param [ BSON::Document ] reply_document The reply document of the
        #   previous message.
        # @param [ Mongo::Server::Connection ] connection The connection being
        #   authenticated.
        #
        # @return [ Protocol::Message ] The next message to send.
        #
        # @since 2.0.0
        def continue(reply_document, connection)
          @nonce = reply_document[Auth::NONCE]
          selector = LOGIN.merge(user: user.name, nonce: nonce, key: user.auth_key(nonce))
          build_message(connection, user.auth_source, selector)
        end
      end
    end
  end
end

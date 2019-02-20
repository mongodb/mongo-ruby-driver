# Copyright (C) 2014-2019 MongoDB Inc.
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
      class Conversation

        # The login message base.
        #
        # @since 2.0.0
        LOGIN = { authenticate: 1 }.freeze

        # @return [ Protocol::Message ] reply The current reply in the
        #   conversation.
        attr_reader :reply

        # @return [ String ] database The database to authenticate against.
        attr_reader :database

        # @return [ String ] nonce The initial auth nonce.
        attr_reader :nonce

        # @return [ User ] user The user for the conversation.
        attr_reader :user

        # Continue the CR conversation. This sends the client final message
        # to the server after setting the reply from the previous server
        # communication.
        #
        # @example Continue the conversation.
        #   conversation.continue(reply)
        #
        # @param [ Protocol::Message ] reply The reply of the previous
        #   message.
        # @param [ Mongo::Server::Connection ] connection The connection being authenticated.
        #
        # @return [ Protocol::Query ] The next message to send.
        #
        # @since 2.0.0
        def continue(reply, connection = nil)
          validate!(reply)
          if connection && connection.features.op_msg_enabled?
            selector = LOGIN.merge(user: user.name, nonce: nonce, key: user.auth_key(nonce))
            selector[Protocol::Msg::DATABASE_IDENTIFIER] = user.auth_source
            cluster_time = connection.mongos? && connection.cluster_time
            selector[Operation::CLUSTER_TIME] = cluster_time if cluster_time
            Protocol::Msg.new([], {}, selector)
          else
            Protocol::Query.new(
              user.auth_source,
              Database::COMMAND,
              LOGIN.merge(user: user.name, nonce: nonce, key: user.auth_key(nonce)),
              limit: -1
            )
          end
        end

        # Finalize the CR conversation. This is meant to be iterated until
        # the provided reply indicates the conversation is finished.
        #
        # @example Finalize the conversation.
        #   conversation.finalize(reply)
        #
        # @param [ Protocol::Message ] reply The reply of the previous
        #   message.
        #
        # @return [ Protocol::Query ] The next message to send.
        #
        # @since 2.0.0
        def finalize(reply, connection = nil)
          validate!(reply)
        end

        # Start the CR conversation. This returns the first message that
        # needs to be sent to the server.
        #
        # @example Start the conversation.
        #   conversation.start
        #
        # @return [ Protocol::Query ] The first CR conversation message.
        #
        # @since 2.0.0
        def start(connection = nil)
          if connection && connection.features.op_msg_enabled?
            selector = Auth::GET_NONCE.merge(Protocol::Msg::DATABASE_IDENTIFIER => user.auth_source)
            cluster_time = connection.mongos? && connection.cluster_time
            selector[Operation::CLUSTER_TIME] = cluster_time if cluster_time
            Protocol::Msg.new([], {}, selector)
          else
            Protocol::Query.new(
              user.auth_source,
              Database::COMMAND,
              Auth::GET_NONCE,
              limit: -1)
          end
        end

        # Create the new conversation.
        #
        # @example Create the new conversation.
        #   Conversation.new(user, "admin")
        #
        # @param [ Auth::User ] user The user to converse about.
        #
        # @since 2.0.0
        def initialize(user)
          @user = user
        end

        private

        def validate!(reply)
          if reply.documents[0][Operation::Result::OK] != 1
            raise Unauthorized.new(user, MECHANISM)
          end
          @nonce = reply.documents[0][Auth::NONCE]
          @reply = reply
        end
      end
    end
  end
end

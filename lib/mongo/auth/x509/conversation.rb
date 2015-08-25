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
    class X509

      # Defines behaviour around a single x.509 conversation between the
      # client and server.
      #
      # @since 2.0.0
      class Conversation

        # The login message.
        #
        # @since 2.0.0
        LOGIN = { authenticate: 1 }.freeze

        # @return [ Protocol::Reply ] reply The current reply in the
        #   conversation.
        attr_reader :reply

        # @return [ User ] user The user for the conversation.
        attr_reader :user

        # Finalize the x.509 conversation. This is meant to be iterated until
        # the provided reply indicates the conversation is finished.
        #
        # @example Finalize the conversation.
        #   conversation.finalize(reply)
        #
        # @param [ Protocol::Reply ] reply The reply of the previous
        #   message.
        #
        # @return [ Protocol::Query ] The next message to send.
        #
        # @since 2.0.0
        def finalize(reply)
          validate!(reply)
        end

        # Start the x.509 conversation. This returns the first message that
        # needs to be send to the server.
        #
        # @example Start the conversation.
        #   conversation.start
        #
        # @return [ Protocol::Query ] The first x.509 conversation message.
        #
        # @since 2.0.0
        def start
          Protocol::Query.new(
            Auth::EXTERNAL,
            Database::COMMAND,
            LOGIN.merge(user: user.name, mechanism: X509::MECHANISM),
            limit: -1
          )
        end

        # Create the new conversation.
        #
        # @example Create the new coversation.
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
          raise Unauthorized.new(user) if reply.documents[0][Operation::Result::OK] != 1
          @reply = reply
        end
      end
    end
  end
end

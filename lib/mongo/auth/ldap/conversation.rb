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
    class LDAP

      # Defines behavior around a single PLAIN conversation between the
      # client and server.
      #
      # @since 2.0.0
      # @api private
      class Conversation < ConversationBase

        # The login message.
        #
        # @since 2.0.0
        LOGIN = { saslStart: 1, autoAuthorize: 1 }.freeze

        # Start the PLAIN conversation. This returns the first message that
        # needs to be sent to the server.
        #
        # @param [ Server::Connection ] connection The connection being
        #   authenticated.
        #
        # @return [ Protocol::Query ] The first PLAIN conversation message.
        #
        # @since 2.0.0
        def start(connection)
          validate_external_auth_source
          selector = LOGIN.merge(payload: payload, mechanism: LDAP::MECHANISM)
          build_message(connection, '$external', selector)
        end

        private

        def payload
          BSON::Binary.new("\x00#{user.name}\x00#{user.password}")
        end
      end
    end
  end
end

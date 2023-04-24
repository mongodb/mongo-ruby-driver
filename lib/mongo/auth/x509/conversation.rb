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
    class X509

      # Defines behavior around a single X.509 conversation between the
      # client and server.
      #
      # @since 2.0.0
      # @api private
      class Conversation < ConversationBase

        # The login message.
        #
        # @since 2.0.0
        LOGIN = { authenticate: 1, mechanism: X509::MECHANISM }.freeze

        # Start the X.509 conversation. This returns the first message that
        # needs to be sent to the server.
        #
        # @param [ Server::Connection ] connection The connection being
        #   authenticated.
        #
        # @return [ Protocol::Message ] The first X.509 conversation message.
        #
        # @since 2.0.0
        def start(connection)
          validate_external_auth_source
          selector = client_first_document
          build_message(connection, '$external', selector)
        end

        # Returns the hash to provide to the server in the handshake
        # as value of the speculativeAuthenticate key.
        #
        # If the auth mechanism does not support speculative authentication,
        # this method returns nil.
        #
        # @return [ Hash | nil ] Speculative authentication document.
        def speculative_auth_document
          client_first_document
        end

        private

        def client_first_document
          LOGIN.dup.tap do |payload|
            payload[:user] = user.name if user.name
          end
        end
      end
    end
  end
end

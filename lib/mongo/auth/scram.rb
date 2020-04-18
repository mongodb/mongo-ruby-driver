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
    # @since 2.0.0
    # @api private
    class SCRAM < Base

      # The authentication mechanism string for SCRAM-SHA-1.
      #
      # @since 2.6.0
      SCRAM_SHA_1_MECHANISM = 'SCRAM-SHA-1'.freeze

      # The authentication mechanism string for SCRAM-SHA-256.
      #
      # @since 2.6.0
      SCRAM_SHA_256_MECHANISM = 'SCRAM-SHA-256'.freeze

      # Map the user-specified authentication mechanism to the proper names of the mechanisms.
      #
      # @since 2.6.0
      MECHANISMS = {
        scram: SCRAM_SHA_1_MECHANISM,
        scram256: SCRAM_SHA_256_MECHANISM,
      }.freeze

      # Log the user in on the given connection.
      #
      # @param [ Mongo::Connection ] connection The connection to log into.
      #
      # @return [ BSON::Document ] The document of the authentication response.
      #
      # @since 2.0.0
      def login(connection)
        mechanism = user.mechanism || :scram
        conversation = Conversation.new(user, mechanism)
        converse_multi_step(connection, conversation).tap do
          unless conversation.server_verified?
            raise Error::MissingScramServerSignature
          end
        end
      end
    end
  end
end

require 'mongo/auth/scram/conversation'

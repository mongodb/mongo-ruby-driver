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

require 'mongo/auth/scram/conversation'

module Mongo
  module Auth

    # Defines behaviour for SCRAM-SHA1 authentication.
    #
    # @since 2.0.0
    class SCRAM
      include Executable

      # The authentication mechinism string.
      #
      # @since 2.0.0
      MECHANISM = 'SCRAM-SHA-1'.freeze

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
        conversation = Conversation.new(user)
        reply = connection.dispatch([ conversation.start ])
        reply = connection.dispatch([ conversation.continue(reply) ])
        reply = connection.dispatch([ conversation.finalize(reply) ])
        reply
      end
    end
  end
end

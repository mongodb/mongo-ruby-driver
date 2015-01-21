# Copyright (C) 2014-2015 MongoDB Inc.
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

require 'mongo/auth/cr/conversation'

module Mongo
  module Auth

    # Defines behaviour for MongoDB-CR authentication.
    #
    # @since 2.0.0
    class CR
      include Executable

      # Log the user in on the given connection.
      #
      # @example Log the user in.
      #   user.login(connection)
      #
      # @param [ Mongo::Connection ] connection The connection to log into.
      #
      # @return [ Protocol::Reply ] The authentication response.
      #
      # @since 2.0.0
      def login(connection)
        conversation = Conversation.new(user, auth_database(connection))
        reply = connection.dispatch([ conversation.start ])
        reply = connection.dispatch([ conversation.continue(reply) ])
        conversation.finalize(reply)
      end
    end
  end
end

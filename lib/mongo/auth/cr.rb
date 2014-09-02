# Copyright (C) 2009 - 2014 MongoDB Inc.
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
        nonce = connection.dispatch([ nonce_message ]).documents[0]
        reply = connection.dispatch([ login_message(nonce[Auth::NONCE]) ])
        raise Unauthorized.new(user) if reply.documents[0]['ok'] == 0
        reply
      end

      private

      # On 2.6 and higher, nonce messages must always go to the admin database,
      # where on 2.4 and lower they go to the database the user is authorized
      # for.
      def nonce_message
        Protocol::Query.new(Database::ADMIN, Database::COMMAND, Auth::GET_NONCE, limit: -1)
      end

      # On 2.6 and higher, login messages must always go to the admin database,
      # where on 2.4 and lower they go to the database the user is authorized
      # for.
      def login_message(nonce)
        Protocol::Query.new(
          Database::ADMIN,
          Database::COMMAND,
          { authenticate: 1, user: user.name, nonce: nonce, key: user.auth_key(nonce) },
          limit: -1
        )
      end
    end
  end
end

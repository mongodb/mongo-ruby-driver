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

require 'mongo/auth/cr/conversation'

module Mongo
  module Auth

    # Defines behavior for MongoDB-CR authentication.
    #
    # @since 2.0.0
    # @deprecated MONGODB-CR authentication mechanism is deprecated
    #   as of MongoDB 3.6. Support for it in the Ruby driver will be
    #   removed in driver version 3.0. Please use SCRAM instead.
    class CR

      # The authentication mechinism string.
      #
      # @since 2.0.0
      MECHANISM = 'MONGODB-CR'.freeze

      # @return [ Mongo::Auth::User ] The user to authenticate.
      attr_reader :user

      # Instantiate a new authenticator.
      #
      # @example Create the authenticator.
      #   Mongo::Auth::CR.new(user)
      #
      # @param [ Mongo::Auth::User ] user The user to authenticate.
      #
      # @since 2.0.0
      def initialize(user)
        @user = user
      end

      # Log the user in on the given connection.
      #
      # @example Log the user in.
      #   user.login(connection)
      #
      # @param [ Mongo::Connection ] connection The connection to log into.
      #
      # @return [ Protocol::Message ] The authentication response.
      #
      # @since 2.0.0
      def login(connection)
        conversation = Conversation.new(user)
        reply = connection.dispatch([ conversation.start(connection) ])
        connection.update_cluster_time(Operation::Result.new(reply))
        reply = connection.dispatch([ conversation.continue(reply, connection) ])
        connection.update_cluster_time(Operation::Result.new(reply))
        conversation.finalize(reply, connection)
      end
    end
  end
end

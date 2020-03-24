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

    # Base class for authenticators.
    #
    # @api private
    class Base

      # @return [ Mongo::Auth::User ] The user to authenticate.
      attr_reader :user

      # Instantiate a new authenticator.
      #
      # @param [ Mongo::Auth::User ] user The user to authenticate.
      #
      # @since 2.0.0
      def initialize(user)
        @user = user
      end

      private

      def converse_1_step(connection, conversation)
        reply = connection.dispatch([ conversation.start(connection) ])
        connection.update_cluster_time(Operation::Result.new(reply))
        conversation.finalize(reply, connection)
      end

      def converse_2_step(connection, conversation)
        reply = connection.dispatch([ conversation.start(connection) ])
        connection.update_cluster_time(Operation::Result.new(reply))
        reply = connection.dispatch([ conversation.continue(reply, connection) ])
        connection.update_cluster_time(Operation::Result.new(reply))
        conversation.finalize(reply, connection)
      end

      # Performs the variable-length SASL conversation on the given connection.
      def converse_multi_step(connection, conversation)
        # Although the SASL conversation in theory can have any number of
        # steps, all defined authentication methods have a predefined number
        # of steps, and therefore all of our authenticators have a fixed set
        # of methods that generate payloads with one method per step.
        # We support a maximum of 3 total exchanges (start, continue and
        # finalize) and in practice the first two exchanges always happen.
        reply = connection.dispatch([ conversation.start(connection) ])
        connection.update_cluster_time(Operation::Result.new(reply))
        reply = connection.dispatch([ conversation.continue(reply, connection) ])
        connection.update_cluster_time(Operation::Result.new(reply))
        unless reply.documents.first[:done]
          reply = connection.dispatch([ conversation.finalize(reply, connection) ])
          connection.update_cluster_time(Operation::Result.new(reply))
        end
        reply
      end
    end
  end
end

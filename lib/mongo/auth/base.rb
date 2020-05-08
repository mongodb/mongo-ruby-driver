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
    # Each authenticator is instantiated for authentication over a particular
    # connection.
    #
    # @api private
    class Base

      # @return [ Mongo::Auth::User ] The user to authenticate.
      attr_reader :user

      # @return [ Mongo::Connection ] The connection to authenticate over.
      attr_reader :connection

      # Initializes the authenticator.
      #
      # @param [ Auth::User ] user The user to authenticate.
      # @param [ Mongo::Connection ] connection The connection to authenticate
      #   over.
      def initialize(user, connection, **opts)
        @user = user
        @connection = connection
      end

      def conversation
        @conversation ||= self.class.const_get(:Conversation).new(user, connection)
      end

      private

      # Performs a single-step conversation on the given connection.
      def converse_1_step(connection, conversation)
        msg = conversation.start(connection)
        dispatch_msg(connection, conversation, msg)
      end

      # Performs a two-step conversation on the given connection.
      #
      # The implementation is very similar to +converse_multi_step+, but
      # conversations using this method do not involve the server replying
      # with {done: true} to indicate the end of the conversation.
      def converse_2_step(connection, conversation)
        msg = conversation.start(connection)
        reply_document = dispatch_msg(connection, conversation, msg)
        msg = conversation.continue(reply_document, connection)
        dispatch_msg(connection, conversation, msg)
      end

      # Performs the variable-length SASL conversation on the given connection.
      #
      # @param [ Server::Connection ] connection The connection.
      # @param [ Auth::*::Conversation ] conversation The conversation.
      # @param [ BSON::Document | nil ] speculative_auth_result The
      #   value of speculativeAuthenticate field of ismaster response of
      #   the handshake on the specified connection.
      def converse_multi_step(connection, conversation,
        speculative_auth_result: nil
      )
        # Although the SASL conversation in theory can have any number of
        # steps, all defined authentication methods have a predefined number
        # of steps, and therefore all of our authenticators have a fixed set
        # of methods that generate payloads with one method per step.
        # We support a maximum of 3 total exchanges (start, continue and
        # finalize) and in practice the first two exchanges always happen.
        if speculative_auth_result
          reply_document = speculative_auth_result
        else
          msg = conversation.start(connection)
          reply_document = dispatch_msg(connection, conversation, msg)
        end
        msg = conversation.continue(reply_document, connection)
        reply_document = dispatch_msg(connection, conversation, msg)
        conversation.process_continue_response(reply_document)
        unless reply_document[:done]
          msg = conversation.finalize(connection)
          reply_document = dispatch_msg(connection, conversation, msg)
        end
        unless reply_document[:done]
          raise Error::InvalidServerAuthResponse,
            'Server did not respond with {done: true} after finalizing the conversation'
        end
        reply_document
      end

      def dispatch_msg(connection, conversation, msg)
        reply = connection.dispatch([msg])
        reply_document = reply.documents.first
        validate_reply!(connection, conversation, reply_document)
        result = Operation::Result.new(reply, connection.description)
        connection.update_cluster_time(result)
        reply_document
      end

      # Checks whether reply is successful (i.e. has {ok: 1} set) and
      # raises Unauthorized if not.
      def validate_reply!(connection, conversation, doc)
        if doc[:ok] != 1
          extra = [doc[:code], doc[:codeName]].compact.join(': ')
          msg = doc[:errmsg]
          unless extra.empty?
            msg += " (#{extra})"
          end
          raise Unauthorized.new(user,
            used_mechanism: self.class.const_get(:MECHANISM),
            message: msg,
            server: connection.server,
          )
        end
      end
    end
  end
end

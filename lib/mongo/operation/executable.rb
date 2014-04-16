# Copyright (C) 2009-2014 MongoDB, Inc.
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

  module Operation

    # This module contains common functionality for defining an operation
    # and the context in which it should be executed in a cluster.
    #
    # @since 3.0.0
    module Executable

      # The specifications describing this operation.
      #
      # @return [ Hash ] The specs for the operation.
      #
      # @since 3.0.0
      attr_reader :spec

      # Check equality of two executable operations.
      #
      # @example Check operation equality.
      #   operation == other
      #
      # @param [ Object ] other The other operation.
      #
      # @return [ true, false ] Whether the objects are equal.
      #
      # @since 3.0.0
      def ==(other)
        spec == other.spec &&
            context == other.context
      end
      alias_method :eql?, :==

      # The context to be used for executing the operation.
      #
      # @return [ Hash ] The context.
      #
      # @since 3.0.0
      def context
        { :server_preference => server_preference ||
            Mongo::Operation::DEFAULT_SERVER_PREFERENCE }.tap do |cxt|
          cxt.merge!(:server => @server)     if @server
        end
      end

      # Execute the operation.
      # The client uses the context to get a server to which the operation
      # is sent in the block.
      # The context contains criteria for which server type to use or which
      # specific server to use.
      #
      # @params [ Mongo::Client ] The client to use to get a server.
      #
      # @todo: Make sure this is indeed the client#with_context API
      # @return [ Array ] The operation results and server used.
      #
      # @since 3.0.0
      def execute(client)
        client.with_context(context) do |server|
          # @todo: check if exhaust and send+receive differently
          server.send_and_receive(message)
        end
      end

      private

      # The name of the database to which the operation should be sent.
      #
      # @return [ String ] Database name.
      #
      # @since 3.0.0
      def db_name
        @spec[:db_name]
      end

      # The name of the collection to which the operation should be sent.
      #
      # @return [ String ] Collection name.
      #
      # @since 3.0.0
      def coll_name
        @spec[:coll_name]
      end
    end
  end
end

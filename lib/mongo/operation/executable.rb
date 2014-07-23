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
    # and executing it, given a certain context.
    #
    # @since 2.0.0
    module Executable

      # The specifications describing this operation.
      #
      # @return [ Hash ] The specs for the operation.
      #
      # @since 2.0.0
      attr_reader :spec

      # Check equality of two executable operations.
      #
      # @example
      #   operation == other
      #
      # @param [ Object ] other The other operation.
      #
      # @return [ true, false ] Whether the objects are equal.
      #
      # @since 2.0.0
      def ==(other)
        spec == other.spec
      end
      alias_method :eql?, :==

      # Execute the operation.
      # The context gets a connection on which the operation
      # is sent in the block.
      #
      # @params [ Mongo::Server::Context ] The context for this operation.
      #
      # @return [ Mongo::Response ] The operation response, if there is one.
      #
      # @since 2.0.0
      def execute(context)
        unless context.primary? || context.standalone? || secondary_ok?
          raise Exception, "Must use primary server"
        end
        context.with_connection do |connection|
          connection.dispatch([message])
        end
      end

      # The name of the database to which the operation should be sent.
      #
      # @return [ String ] Database name.
      #
      # @since 2.0.0
      def db_name
        @spec[:db_name]
      end

      # The name of the collection to which the operation should be sent.
      #
      # @return [ String ] Collection name.
      #
      # @since 2.0.0
      def coll_name
        @spec[:coll_name]
      end

      # Merge this operation with another operation, returning a new one.
      # Requires that the collection and database of the two ops are the same.
      #
      # @params[ Object ] The other operation.
      #
      # @return [ Object ] A new operation merging this one and another.
      #
      # @since 2.0.0
      def merge(other)
        # @todo: use specific exception
        raise Exception, "Cannot merge" unless self.class == other.class &&
            coll_name == other.coll_name &&
            db_name == other.db_name
        dup.merge!(other)
      end

      # If an operation including this module doesn't define #merge!, neither
      # #merge nor #merge! will be allowed.
      #
      # @params[ Object ] The other operation.
      #
      # @raise [ Exception ] Merging is not supported for this operation.
      #
      # @since 2.0.0
      def merge!(other)
        raise Exception, "Merging not allowed for this operation type"
      end

      private

      # If it's ok that this operation be sent to a secondary server.
      #
      # @return [ true, false ] Whether it's ok for this op to go to a secondary.
      #
      # @since 2.0.0
      def secondary_ok?
        true
      end
    end
  end
end

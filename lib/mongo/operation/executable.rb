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

      # Get the index from the specification.
      #
      # @return [ Hash ] The index specification.
      #
      # @since 2.0.0
      def index
        @spec[:index]
      end

      # Get the index name from the spec.
      #
      # @return [ String ] The index name.
      #
      # @since 2.0.0
      def index_name
        @spec[:index_name]
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

      # Get the full namespace that this operates on.
      #
      # @example Get the namespace.
      #   executable.namespace
      #
      # @return [ String ] The namespace.
      #
      # @since 2.0.0
      def namespace
        "#{db_name}.#{coll_name}"
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

      # The options for the executable.
      #
      # @return [ Hash ] The executable options.
      #
      # @since 2.0.0
      def options
        @spec[:opts] || {}
      end

      # The write concern to use for this operation.
      #
      # @return [ Mongo::WriteConcern::Mode ] The write concern.
      #
      # @since 2.0.0
      def write_concern
        @spec[:write_concern] || WriteConcern::Mode.get(WriteConcern::Mode::DEFAULT)
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

      # Gets the legacy get last error command as a wire protocol query.
      #
      # @since 2.0.0
      def gle
        if gle_message = write_concern.get_last_error
          Protocol::Query.new(
            db_name,
            Database::COMMAND,
            gle_message,
            options.merge(limit: -1)
          )
        end
      end
    end
  end
end

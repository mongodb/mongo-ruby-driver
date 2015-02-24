# Copyright (C) 2014-2015 MongoDB, Inc.
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

      # Execute the operation.
      # The context gets a connection on which the operation
      # is sent in the block.
      #
      # @param [ Mongo::Server::Context ] context The context for this operation.
      #
      # @return [ Result ] The operation response, if there is one.
      #
      # @since 2.0.0
      def execute(context)
        context.with_connection do |connection|
          connection.dispatch([ message ])
        end
      end

      # Merge this operation with another operation, returning a new one.
      # Requires that the collection and database of the two ops are the same.
      #
      # @param[ Object ] The other operation.
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
      # @param[ Object ] The other operation.
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

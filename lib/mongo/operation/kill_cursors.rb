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

    # A MongoDB kill cursors operation with context describing
    # what server or socket it should be sent to.
    #
    # @since 3.0.0
    class KillCursors
      include Executable

      # The ids of the cursors to kill.
      #
      # @return [Array] Ids of the cursors to kill.
      attr_reader :cursor_ids

      # Initialize the kill cursors operation.
      #
      # @example Initialize a kill cursors operation.
      #   Mongo::Operation::KillCursor.new([1, 2], { :server => server })
      #
      # @param [ Hash ] cursor_ids Ids of the cursors to kill.
      # @param [ Hash ] context The context for executing this operation.
      #
      # @option context :server [ Mongo::Server ] The server to use for the operation.
      # @option context :connection [ Mongo::Socket ] The socket that the operation
      #   message should be sent on.
      #
      # @since 3.0.0
      def initialize(spec, context={})
        # @todo: Replace with appropriate error
        # @todo: can you specify a connection?
        raise Exception, 'You must specify a server' unless @server = context[:server]
        @spec       = spec
        @connection = context[:connection]
      end

      # The context to be used for executing the operation.
      #
      # @return [ Hash ] The context.
      #
      # @since 3.0.0
      def context
        { :server     => @server,
          :connection => @connection }
      end

      private

      # The ids of the cursors to kill.
      #
      # @return [ Array ] The cursor ids.
      #
      # @since 3.0.0
      def cursor_ids
        @spec[:cursor_ids]
      end

      # The wire protocol message for this kill cursors operation.
      #
      # @return [ Mongo::Protocol::KillCursors ] Wire protocol message.
      #
      # @since 3.0.0
      def message
        Mongo::Protocol::KillCursors.new(cursor_ids)
      end
    end
  end
end

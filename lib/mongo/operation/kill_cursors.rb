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

    # A MongoDB kill cursors operation.
    #
    # @example Create the kill cursors operation.
    #   Mongo::Operation::KillCursor.new({ :cursor_ids => [1, 2] })
    #
    # @param [ Hash ] spec The specifications for the operation.
    #
    # @option spec :cursor_ids [ Array ] The ids of cursors to kill.
    #
    # @since 2.0.0
    class KillCursors
      include Specifiable

      # Execute the operation.
      # The context gets a connection on which the operation
      # is sent in the block.
      #
      # @params [ Mongo::Server::Context ] The context for this operation.
      #
      # @return [ Result ] The operation response, if there is one.
      #
      # @since 2.0.0
      def execute(context)
        execute_message(context)
      end

      private

      def execute_message(context)
        context.with_connection do |connection|
          Result.new(connection.dispatch([ message ]))
        end
      end

      def message
        Protocol::KillCursors.new(cursor_ids)
      end
    end
  end
end

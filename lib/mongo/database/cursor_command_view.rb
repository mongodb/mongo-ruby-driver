# frozen_string_literal: true

# Copyright (C) 2025 MongoDB Inc.
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
  class Database
    # The minimal view a Cursor needs when it is built from an arbitrary
    # command response rather than from a collection query.
    #
    # It carries the getMore-specific options (batchSize, maxTimeMS, comment)
    # and the cursor type and timeout mode, and answers the few methods the
    # Cursor reads from its view.
    #
    # @api private
    class CursorCommandView
      # @param [ Mongo::Database ] database The database the command ran on.
      # @param [ Hash ] options The getMore and timeout options.
      #
      # @option options [ Integer ] :batch_size The batchSize for getMores.
      # @option options [ Integer ] :max_time_ms The maxTimeMS for getMores.
      # @option options [ Object ] :comment The comment for getMores.
      # @option options [ Symbol ] :cursor_type :tailable or :tailable_await.
      # @option options [ Symbol ] :timeout_mode :cursor_lifetime or :iteration.
      def initialize(database, options = {})
        @database = database
        @options = options
      end

      # @return [ Mongo::Database ] The database the command ran on.
      attr_reader :database

      # @return [ Hash ] The view options. Used by the Cursor to read the
      #   getMore comment.
      attr_reader :options

      # @return [ Mongo::Client ] The client.
      def client
        database.client
      end

      # A placeholder collection used only so the Cursor can reach the client
      # and database. The actual namespace for getMore and killCursors is taken
      # from the command response, not from this collection.
      #
      # @return [ Mongo::Collection ] The $cmd pseudo collection.
      def collection
        @collection ||= Collection.new(database, '$cmd')
      end

      # @return [ Integer | nil ] The batchSize sent on getMore commands.
      def batch_size
        options[:batch_size]
      end

      # @return [ Integer | nil ] The maxTimeMS sent on getMore commands.
      def max_time_ms_for_get_more
        options[:max_time_ms]
      end

      # @return [ Symbol | nil ] The cursor type.
      def cursor_type
        options[:cursor_type]
      end

      # @return [ Symbol | nil ] The timeout mode.
      def timeout_mode
        options[:timeout_mode]
      end

      # @return [ Hash ] timeout values for the operation context.
      def operation_timeouts(opts = {})
        database.operation_timeouts(opts)
      end

      private

      # Cursors do not support a limit when built from a command response.
      def limit
        nil
      end
    end
  end
end

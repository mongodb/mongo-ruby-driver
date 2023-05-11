# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2021 MongoDB Inc.
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
  class Cursor

    # This class contains the operation specification for KillCursors.
    #
    # Its purpose is to ensure we don't misspell attribute names accidentally.
    #
    # @api private
    class KillSpec

      def initialize(
        cursor_id:,
        coll_name:,
        db_name:,
        connection_global_id:,
        server_address:,
        session:
      )
        @cursor_id = cursor_id
        @coll_name = coll_name
        @db_name = db_name
        @connection_global_id = connection_global_id
        @server_address = server_address
        @session = session
      end

      attr_reader :cursor_id,
      :coll_name,
      :db_name,
      :connection_global_id,
      :server_address,
      :session

      def ==(other)
        cursor_id == other.cursor_id &&
          coll_name == other.coll_name &&
          db_name == other.db_name &&
          connection_global_id == other.connection_global_id &&
          server_address == other.server_address &&
          session == other.session
      end

      def eql?(other)
        self.==(other)
      end

      def hash
        [
          cursor_id,
          coll_name,
          db_name,
          connection_global_id,
          server_address,
          session,
        ].compact.hash
      end
    end
  end
end

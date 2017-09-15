# Copyright (C) 2017 MongoDB, Inc.
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

  class Session

    # An object representing the server-side session.
    #
    # @api private
    #
    # @since 2.5.0
    class ServerSession

      # The last time the server session was used.
      #
      # @since 2.5.0
      attr_reader :last_use

      # Initialize a ServerSession.
      #
      # @example
      #   ServerSession.new
      #
      # @since 2.5.0
      def initialize
        @last_use = Time.now
      end

      # Update the last_use attribute of the server session to now.
      #
      # @example Set the last use field to now.
      #   server_session.set_last_use!
      #
      # @since 2.5.0
      def set_last_use!
        @last_use = Time.now
      end

      # The session id of this server session.
      #
      # @example Get the session id.
      #   server_session.session_id
      #
      # @since 2.5.0
      def session_id
        @session_id ||= { id: BSON::Binary.new("p4\x8F]\xB8\xCDI*\xA1q2A\x91\xC0\xABd", :uuid) }
      end
    end
  end
end
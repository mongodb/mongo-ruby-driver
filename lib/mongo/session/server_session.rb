# Copyright (C) 2017-2019 MongoDB, Inc.
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

      # Regex for removing dashes from the UUID string.
      #
      # @since 2.5.0
      DASH_REGEX = /\-/.freeze

      # Pack directive for the UUID.
      #
      # @since 2.5.0
      UUID_PACK = 'H*'.freeze

      # The last time the server session was used.
      #
      # @since 2.5.0
      attr_reader :last_use

      # The current transaction number.
      #
      # When a transaction is active, all operations in that transaction
      # use the same transaction number. If the entire transaction is restarted
      # (for example, by Session#with_transaction, in which case it would
      # also invoke the block provided to it again), each transaction attempt
      # has its own transaction number.
      #
      # Transaction number is also used outside of transactions for
      # retryable writes. In this case, each write operation has its own
      # transaction number, but retries of a write operation use the same
      # transaction number as the first write (which is how the server
      # knows that subsequent writes are retries and should be ignored if
      # the first write succeeded on the server but was not read by the
      # client, for example).
      #
      # @since 2.5.0
      attr_reader :txn_num

      # Initialize a ServerSession.
      #
      # @example
      #   ServerSession.new
      #
      # @since 2.5.0
      def initialize
        set_last_use!
        session_id
        @txn_num = 0
      end

      # Update the last_use attribute of the server session to now.
      #
      # @example Set the last use field to now.
      #   server_session.set_last_use!
      #
      # @return [ Time ] The last time the session was used.
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
      # @return [ BSON::Document ] The session id.
      #
      # @since 2.5.0
      def session_id
        @session_id ||= (bytes = [SecureRandom.uuid.gsub(DASH_REGEX, '')].pack(UUID_PACK)
                          BSON::Document.new(id: BSON::Binary.new(bytes, :uuid)))
      end

      # Increment the current transaction number and return the new value.
      #
      # @return [ Integer ] The updated transaction number.
      #
      # @since 2.5.0
      def next_txn_num
        @txn_num += 1
      end

      # Get a formatted string for use in inspection.
      #
      # @example Inspect the session object.
      #   session.inspect
      #
      # @return [ String ] The session inspection.
      #
      # @since 2.5.0
      def inspect
        "#<Mongo::Session::ServerSession:0x#{object_id} session_id=#{session_id} last_use=#{@last_use}>"
      end
    end
  end
end

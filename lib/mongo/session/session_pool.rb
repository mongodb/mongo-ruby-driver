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

    # A pool of server sessions.
    #
    # @api private
    #
    # @since 2.5.0
    class SessionPool

      # Create a SessionPool.
      #
      # @example
      #   SessionPool.create(client)
      #
      # @param [ Mongo::Client ] client The client that will be associated with this
      #   session pool.
      #
      # @since 2.5.0
      def self.create(client)
        pool = new(client)
        client.instance_variable_set(:@session_pool, pool)
      end

      # Initialize a SessionPool.
      #
      # @example
      #   SessionPool.new(client)
      #
      # @param [ Mongo::Client ] client The client that will be associated with this
      #   session pool.
      #
      # @since 2.5.0
      def initialize(client)
        @queue = []
        @mutex = Mutex.new
        @client = client
      end

      # Checkout a session to be used in the context of a block and return the session back to
      #   the pool after the block completes.
      #
      # @example Checkout, use a session, and return it back to the pool after the block.
      #   pool.with_session do |session|
      #     ...
      #   end
      #
      # @yieldparam [ ServerSession ] The server session.
      #
      # @since 2.5.0
      def with_session
        server_session = checkout
        yield(server_session)
      ensure
        begin; checkin(server_session) if server_session; rescue; end
      end

      # Checkout a server session from the pool.
      #
      # @example Checkout a session.
      #   pool.checkout
      #
      # @return [ ServerSession ] The server session.
      #
      # @since 2.5.0
      def checkout
        @mutex.synchronize do
          loop do
            if @queue.empty?
              return ServerSession.new
            else
              session = @queue.shift
              unless about_to_expire?(session)
                return session
              end
            end
          end
        end
      end

      # Checkin a server session to the pool.
      #
      # @example Checkin a session.
      #   pool.checkin(session)
      #
      # @param [ Session::ServerSession ] session The session to checkin.
      #
      # @since 2.5.0
      def checkin(session)
        @mutex.synchronize do
          prune!
          unless about_to_expire?(session)
            @queue.unshift(session)
          end
        end
      end

      # End all sessions in the pool by sending the endSessions command to the server.
      #
      # @example End all sessions.
      #   pool.end_sessions
      #
      # @since 2.5.0
      def end_sessions
        if @client
          server = ServerSelector.get(mode: :primary_preferred).select_server(@client.cluster)
          while !@queue.empty?
            Operation::Commands::Command.new(
                :selector => {endSessions: @queue.shift(10_000).collect { |s| s.session_id }},
                :db_name => Database::ADMIN).execute(server)
          end
        end
      rescue
      end

      private

      def about_to_expire?(session)
        if @client.logical_session_timeout
          idle_time_minutes = (Time.now - session.last_use) / 60
          (idle_time_minutes + 1) >= @client.logical_session_timeout
        end
      end

      def prune!
        while !@queue.empty?
          if about_to_expire?(@queue[-1])
            @queue.pop
          else
            break
          end
        end
      end
    end
  end
end

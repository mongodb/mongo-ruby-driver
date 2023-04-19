# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2017-2020 MongoDB Inc.
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
      #   SessionPool.create(cluster)
      #
      # @param [ Mongo::Cluster ] cluster The cluster that will be associated with this
      #   session pool.
      #
      # @since 2.5.0
      def self.create(cluster)
        pool = new(cluster)
        cluster.instance_variable_set(:@session_pool, pool)
      end

      # Initialize a SessionPool.
      #
      # @example
      #   SessionPool.new(cluster)
      #
      # @param [ Mongo::Cluster ] cluster The cluster that will be associated with this
      #   session pool.
      #
      # @since 2.5.0
      def initialize(cluster)
        @queue = []
        @mutex = Mutex.new
        @cluster = cluster
      end

      # Get a formatted string for use in inspection.
      #
      # @example Inspect the session pool object.
      #   session_pool.inspect
      #
      # @return [ String ] The session pool inspection.
      #
      # @since 2.5.0
      def inspect
        "#<Mongo::Session::SessionPool:0x#{object_id} current_size=#{@queue.size}>"
      end

      # Check out a server session from the pool.
      #
      # @example Check out a session.
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
        if session.nil?
          raise ArgumentError, 'session cannot be nil'
        end

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
        while !@queue.empty?
          server = ServerSelector.get(mode: :primary_preferred).select_server(@cluster)
          op = Operation::Command.new(
            selector: {
              endSessions: @queue.shift(10_000).map(&:session_id),
            },
            db_name: Database::ADMIN,
          )
          context = Operation::Context.new(options: {
            server_api: server.options[:server_api],
          })
          op.execute(server, context: context)
        end
      rescue Mongo::Error, Error::AuthError
      end

      private

      def about_to_expire?(session)
        if session.nil?
          raise ArgumentError, 'session cannot be nil'
        end

        # Load balancers spec explicitly requires to ignore the logical session
        # timeout value.
        # No rationale is provided as of the time of this writing.
        if @cluster.load_balanced?
          return false
        end

        logical_session_timeout = @cluster.logical_session_timeout

        if logical_session_timeout
          idle_time_minutes = (Time.now - session.last_use) / 60
          (idle_time_minutes + 1) >= logical_session_timeout
        end
      end

      def prune!
        # Load balancers spec explicitly requires not to prune sessions.
        # No rationale is provided as of the time of this writing.
        return if @cluster.load_balanced?

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

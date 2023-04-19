# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2014-2020 MongoDB Inc.
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

  class Cluster

    # A manager that calls a method on each of a cluster's pools to close idle
    #  sockets.
    #
    # @api private
    #
    # @since 2.5.0
    class SocketReaper

      # Initialize the SocketReaper object.
      #
      # @example Initialize the socket reaper.
      #   SocketReaper.new(cluster)
      #
      # @param [ Mongo::Cluster ] cluster The cluster whose pools' idle sockets
      #  need to be reaped at regular intervals.
      #
      # @since 2.5.0
      def initialize(cluster)
        @cluster = cluster
      end

      # Execute the operation to close the pool's idle sockets.
      #
      # @example Close the idle sockets in each of the cluster's pools.
      #   socket_reaper.execute
      #
      # @since 2.5.0
      def execute
        @cluster.servers.each do |server|
          server.pool_internal&.close_idle_sockets
        end
        true
      end

      # When the socket reaper is garbage-collected, there's no need to close
      #   idle sockets; sockets will be closed anyway when the pools are
      #   garbage collected.
      #
      # @since 2.5.0
      def flush
      end
    end
  end
end

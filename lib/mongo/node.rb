# Copyright (C) 2009-2013 MongoDB, Inc.
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

require 'mongo/node/refresh'

module Mongo

  # Represents a single node on the server side that can be standalone, part of
  # a replica set, or a mongos.
  #
  # @since 2.0.0
  class Node

    # The default time for a node to refresh its status is 5 seconds.
    #
    # @since 2.0.0
    REFRESH_INTERVAL = 5.freeze

    # The command used for determining node status.
    #
    # @since 2.0.0
    STATUS = { :ismaster => 1 }.freeze

    # @return [ String ] The configured address for the node.
    attr_reader :address
    # @return [ Mongo::Cluster ] The cluster the node belongs to.
    attr_reader :cluster
    # @return [ Mutex ] The refresh operation mutex.
    attr_reader :mutex
    # @return [ Hash ] The options hash.
    attr_reader :options

    def ==(other)
      address == other.address
    end

    # Returns whether or not the node is alive - ie it is connected to and
    # healthy.
    #
    # @example Is the node alive?
    #   node.alive?
    #
    # @return [ true, false ] If the node is alive and healthy.
    #
    # @since 2.0.0
    def alive?
      !!@alive
    end

    # @todo: Send the operation to the connection.
    def execute(operation)

    end

    def initialize(cluster, address, options = {})
      @cluster = cluster
      @address = address
      @options = options
      @mutex = Mutex.new
      @refresh = Refresh.new(self, refresh_interval)
      @refresh.run
    end

    # @todo This should be synchronized. I envison this checks if the node is
    # alive and a primary or secondary. (no arbiters)
    def operable?
      mutex.synchronize do
        true
      end
    end

    def refresh
      mutex.synchronize do
        p 'Refreshing node...'
      end
    end

    # Get the refresh interval for the node. This will be defined via an option
    # or will default to 5.
    #
    # @example Get the refresh interval.
    #   node.refresh_interval
    #
    # @return [ Integer ] The refresh interval, in seconds.
    #
    # @since 2.0.0
    def refresh_interval
      @refresh_interval ||= options[:refresh_interval] || REFRESH_INTERVAL
    end

    private

    # Gets the wire protocol query that will be used to send when refreshing.
    #
    # @api private
    #
    # @return [ Mongo::Protocol::Query ] The refresh command.
    #
    # @since 2.0.0
    def refresh_command
      Protocol::Query.new(
        Database::ADMIN,
        Database::COMMAND,
        STATUS,
        :limit => -1, :read => cluster.client.read_preference
      )
    end
  end
end

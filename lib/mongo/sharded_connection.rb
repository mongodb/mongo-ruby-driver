# encoding: UTF-8

# --
# Copyright (C) 2008-2011 10gen Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ++

module Mongo

  # Instantiates and manages connections to a MongoDB sharded cluster for high availability.
  class ShardedConnection < Connection

    SHAREDED_CLUSTER_OPTS = [:read, :refresh_mode, :refresh_interval, :name]

    attr_reader :sharded_cluster_name, :seeds, :refresh_interval, :refresh_mode,
                :refresh_version, :manager

    # Create a connection to a MongoDB sharded cluster.
    #
    # If no args are provided, it will check <code>ENV["MONGODB_URI"]</code>.
    #
    # @param [Array] seeds "host:port" strings
    #
    # @option opts [String] :name (nil) The name of the sharded cluster to connect to. You
    #   can use this option to verify that you're connecting to the right sharded cluster.
    # @option opts [Boolean, Hash] :safe (false) Set the default safe-mode options
    #   propagated to DB objects instantiated off of this Connection. This
    #   default can be overridden upon instantiation of any DB by explicitly setting a :safe value
    #   on initialization.
    # @option opts [Logger] :logger (nil) Logger instance to receive driver operation log.
    # @option opts [Integer] :pool_size (1) The maximum number of socket connections allowed per
    #   connection pool. Note: this setting is relevant only for multi-threaded applications.
    # @option opts [Float] :pool_timeout (5.0) When all of the connections a pool are checked out,
    #   this is the number of seconds to wait for a new connection to be released before throwing an exception.
    #   Note: this setting is relevant only for multi-threaded applications.
    # @option opts [Float] :op_timeout (nil) The number of seconds to wait for a read operation to time out.
    # @option opts [Float] :connect_timeout (30) The number of seconds to wait before timing out a
    #   connection attempt.
    # @option opts [Boolean] :ssl (false) If true, create the connection to the server using SSL.
    # @option opts [Boolean] :refresh_mode (false) Set this to :sync to periodically update the
    #   state of the connection every :refresh_interval seconds. Sharded cluster connection failures
    #   will always trigger a complete refresh. This option is useful when you want to add new nodes
    #   or remove sharded cluster nodes not currently in use by the driver.
    # @option opts [Integer] :refresh_interval (90) If :refresh_mode is enabled, this is the number of seconds
    #   between calls to check the sharded cluster's state.
    # Note: that the number of seed nodes does not have to be equal to the number of sharded cluster members.
    # The purpose of seed nodes is to permit the driver to find at least one sharded cluster member even if a member is down.
    #
    # @example Connect to a sharded cluster and provide two seed nodes.
    #   Mongo::ShardedConnection.new(['localhost:30000', 'localhost:30001'])
    #
    # @example Connect to a sharded cluster providing two seed nodes and ensuring a connection to the sharded cluster named 'prod':
    #   Mongo::ShardedConnection.new(['localhost:30000', 'localhost:30001'], :name => 'prod')
    #
    # @raise [MongoArgumentError] This is raised for usage errors.
    #
    # @raise [ConnectionFailure] This is raised for the various connection failures.
  end
end

# Copyright (C) 2009-2014 MongoDB, Inc.
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

require 'mongo/server/address'
require 'mongo/server/context'
require 'mongo/server/description'
require 'mongo/server/monitor'

module Mongo

  # Represents a single server on the server side that can be standalone, part of
  # a replica set, or a mongos.
  #
  # @since 2.0.0
  class Server
    include Event::Publisher
    extend Forwardable

    # @return [ String ] The configured address for the server.
    attr_reader :address
    # @return [ Server::Description ] The description of the server.
    attr_reader :description
    # @return [ Hash ] The options hash.
    attr_reader :options

    def_delegators :@description,
                   :max_wire_version,
                   :mongos?,
                   :primary?,
                   :replica_set_name,
                   :secondary?,
                   :standalone?,
                   :write_command_enabled?

    # Is this server equal to another?
    #
    # @example Is the server equal to the other?
    #   server == other
    #
    # @param [ Object ] other The object to compare to.
    #
    # @return [ true, false ] If the servers are equal.
    #
    # @since 2.0.0
    def ==(other)
      return false unless other.is_a?(Server)
      address == other.address
    end

    # Get a new context for this server in which to send messages.
    #
    # @example Get the server context.
    #   server.context
    #
    # @return [ Mongo::Server::Context ] The server context.
    #
    # @since 2.0.0
    def context
      Context.new(self)
    end

    # Disconnect the server from the connection.
    #
    # @example Disconnect the server.
    #   server.disconnect!
    #
    # @return [ true ] Always tru with no exception.
    #
    # @since 2.0.0
    def disconnect!
      context.with_connection do |connection|
        connection.disconnect!
      end and true
    end

    # Instantiate a new server object. Will start the background refresh and
    # subscribe to the appropriate events.
    #
    # @example Initialize the server.
    #   Mongo::Server.new('127.0.0.1:27017')
    #
    # @param [ String ] address The host:port address to connect to.
    # @param [ Hash ] options The server options.
    #
    # @since 2.0.0
    def initialize(address, options = {})
      @address = Address.new(address)
      @options = options
      @mutex = Mutex.new
      @monitor = Monitor.new(self, options)
      @description = Description.new(self)
      @monitor.run
    end

    # Get a pretty printed server inspection.
    #
    # @example Get the server inspection.
    #   server.inspec
    #
    # @return [ String ] The nice inspection string.
    #
    # @since 2.0.0
    def inspect
      "#<Mongo::Server:0x#{object_id} address=#{address.host}:#{address.port}"
    end

    # Get the connection pool for this server.
    #
    # @example Get the connection pool for the server.
    #   server.pool
    #
    # @return [ Mongo::Pool ] The connection pool.
    #
    # @since 2.0.0
    def pool
      @pool ||= Pool.get(self)
    end

    # Is this server able to handle write commands?
    #
    # @example Can the server handle write commands?
    #   server.write_command_ready?
    #
    # @return [ true, false ] If the server can handle write commands.
    #
    # @since 2.0.0
    def write_command_ready?
      description.max_wire_version >= 2
    end
  end
end

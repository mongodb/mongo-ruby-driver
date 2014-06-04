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
require 'mongo/server/description'
require 'mongo/server/monitor'

module Mongo

  # Represents a single server on the server side that can be standalone, part of
  # a replica set, or a mongos.
  #
  # @since 2.0.0
  class Server
    include Event::Publisher
    include Event::Subscriber

    # Error message for Unconnected errors.
    #
    # @since 3.0.0
    UNCONNECTED = 'Server is currently not connected.'.freeze

    # @return [ String ] The configured address for the server.
    attr_reader :address
    # @return [ Server::Description ] The description of the server.
    attr_reader :description
    # @return [ Mutex ] The refresh operation mutex.
    attr_reader :mutex
    # @return [ Hash ] The options hash.
    attr_reader :options

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

    # Tells the monitor to immediately check the server status.
    #
    # @example Check the server status.
    #   server.check!
    #
    # @return [ Server::Description ] The updated server description.
    #
    # @since 2.0.0
    def check!
      @monitor.check!
    end

    # Dispatch the provided messages to the server. If the last message
    # requires a response a reply will be returned.
    #
    # @example Dispatch the messages.
    #   server.dispatch([ insert, command ])
    #
    # @note This method is named dispatch since 'send' is a core Ruby method on
    #   all objects.
    #
    # @param [ Array<Message> ] messages The messages to dispatch.
    #
    # @return [ Protocol::Reply ] The reply if needed.
    #
    # @since 3.0.0
    def dispatch(messages)
      raise Unconnected, UNCONNECTED unless description
      send_messages(messages)
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
      @description = Description.new
      subscribe_to(description, Event::HOST_ADDED, Event::HostAdded.new(self))
      subscribe_to(description, Event::HOST_REMOVED, Event::HostRemoved.new(self))
      # @monitor.run
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

    # Is the server able to receive messages?
    #
    # @example Is the server operable?
    #   server.operable?
    #
    # @note This is true only for a connected server that is a secondary or
    #   primary and not hidden.
    #
    # @return [ true, false ] If the server is operable.
    #
    # @since 2.0.0
    def operable?
      return false if description.unknown? || description.hidden?
      description.primary? || description.secondary?
    end

    # Raised when trying to dispatch a message when the server is not
    # connected.
    #
    # @since 3.0.0
    class Unconnected < RuntimeError; end

    private

    def pool
      @pool ||= Pool.get(self)
    end

    def send_messages(messages)
      mutex.synchronize do
        pool.with_connection do |connection|
          connection.write(messages)
          connection.read if messages.last.replyable?
        end
      end
    end
  end
end

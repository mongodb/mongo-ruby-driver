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
require 'mongo/server/refresh'

module Mongo

  # Represents a single server on the server side that can be standalone, part of
  # a replica set, or a mongos.
  #
  # @since 3.0.0
  class Server
    include Event::Publisher

    # The default time for a server to refresh its status is 5 seconds.
    #
    # @since 3.0.0
    REFRESH_INTERVAL = 5.freeze

    # The command used for determining server status.
    #
    # @since 3.0.0
    STATUS = { :ismaster => 1 }.freeze

    # @return [ String ] The configured address for the server.
    attr_reader :address
    # @return [ Server::Description ] The description of the server.
    attr_reader :description
    # @return [ Mutex ] The refresh operation mutex.
    attr_reader :mutex
    # @return [ Hash ] The options hash.
    attr_reader :options

    def ==(other)
      address == other.address
    end

    def initialize(address, options = {})
      @address = Address.new(address)
      @options = options
      @mutex = Mutex.new
      @refresh = Refresh.new(self, refresh_interval)
      @refresh.run
    end

    def operable?
      true
    end

    def refresh!
      mutex.synchronize do
        # Update the server description here. For changes in the description to
        # the previous we need to fire events.
        #
        # refreshed = Description.new(read(refresh_command))
        #
        # publish(Event::SERVER_ADDED, address)
        # publish(Event::SERVER_REMOVED, address)
      end
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
      with_connection do |connection|
        connection.write(messages)
        connection.read if messages.last.replyable?
      end
    end

    # Get the refresh interval for the server. This will be defined via an option
    # or will default to 5.
    #
    # @example Get the refresh interval.
    #   server.refresh_interval
    #
    # @return [ Integer ] The refresh interval, in seconds.
    #
    # @since 3.0.0
    def refresh_interval
      @refresh_interval ||= options[:refresh_interval] || REFRESH_INTERVAL
    end

    private

    def pool
      @pool ||= Pool.get(self)
    end

    def refresh_command
      Protocol::Query.new(
        Database::ADMIN,
        Database::COMMAND,
        STATUS,
        :limit => -1, :read => cluster.client.read_preference
      )
    end

    def with_connection
      pool.with_connection { |conn| yield(conn) }
    end
  end
end

# Copyright (C) 2009-2013 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

require 'mongo/write_concern/mode'
require 'mongo/write_concern/acknowledged'
require 'mongo/write_concern/unacknowledged'

module Mongo

  # The client is the entry point to the driver and is the main object that
  # will be interacted with.
  #
  # @since 2.0.0
  class Client

    # @return [ Mongo::Cluster ] The cluster of nodes for the client.
    attr_reader :cluster
    # @return [ Hash ] The configuration options.
    attr_reader :options

    # Determine if this client is equivalent to another object.
    #
    # @example Check client equality.
    #   client == other
    #
    # @param [ Object ] other The object to compare to.
    #
    # @return [ true, false ] If the objects are equal.
    #
    # @since 2.0.0
    def ==(other)
      return false unless other.is_a?(Client)
      cluster == other.cluster && options == other.options
    end
    alias_method :eql?, :==

    # Get a collection object for the provided collection name.
    #
    # @example Get the collection.
    #   client[:users]
    #
    # @param [ String, Symbol ] collection_name The name of the collection.
    #
    # @return [ Mongo::Collection ] The collection.
    #
    # @since 2.0.0
    def [](collection_name)
      database[collection_name]
    end

    # Get the hash value of the client.
    #
    # @example Get the client hash value.
    #   client.hash
    #
    # @return [ Integer ] The client hash value.
    #
    # @since 2.0.0
    def hash
      [cluster, options].hash
    end

    # Instantiate a new driver client.
    #
    # @example Instantiate a single node or mongos client.
    #   Mongo::Client.new([ '127.0.0.1:27017' ])
    #
    # @example Instantiate a client for a replica set.
    #   Mongo::Client.new([ '127.0.0.1:27017', '127.0.0.1:27021' ])
    #
    # @param [ Array<String> ] addresses The array of server addresses in the
    #   form of host:port.
    # @param [ Hash ] options The options to be used by the client.
    #
    # @since 2.0.0
    def initialize(addresses, options = {})
      @cluster = Cluster.new(addresses)
      @options = options
      db = options[:database]
      use(db) if db
    end

    # Get an inspection of the client as a string.
    #
    # @example Inspect the client.
    #   client.inspect
    #
    # @return [ String ] The inspection string.
    #
    # @since 2.0.0
    def inspect
      "<Mongo::Client:0x#{object_id} cluster=#{cluster.addresses.join(', ')}>"
    end

    # Use the database with the provided name. This will switch the current
    # database the client is operating on.
    #
    # @example Use the provided database.
    #   client.use(:users)
    #
    # @param [ String, Symbol ] database_name The name of the database to use.
    #
    # @return [ Mongo::Database ] The database now being used.
    #
    # @since 2.0.0
    def use(database_name)
      @database = Database.new(self, database_name)
    end

    # Provides a new client with the passed options merged over the existing
    # options of this client. Useful for one-offs to change specific options
    # without altering the original client.
    #
    # @example Get a client with changed options.
    #   client.with(:read => :primary_preferred)
    #
    # @param [ Hash ] new_options The new options to use.
    #
    # @return [ Mongo::Client ] A new client instance.
    #
    # @since 2.0.0
    def with(new_options = {})
      Client.new(cluster.addresses.dup, options.merge(new_options))
    end

    # Get the write concern for this client. If no option was provided, then a
    # default single server acknowledgement will be used.
    #
    # @example Get the client write concern.
    #   client.write_concern
    #
    # @return [ Mongo::WriteConcern::Mode ] The write concern.
    #
    # @since 2.0.0
    def write_concern
      @write_concern ||= WriteConcern::Mode.get(options[:write])
    end

    # Exception that is raised when trying to perform operations before ever
    # telling the client which database to execute ops on.
    #
    # @since 2.0.0
    class NoDatabase < DriverError

      # The message does not need to be dynamic, so is held in a constant.
      #
      # @since 2.0.0
      MESSAGE = 'No database has been set to operate on in the client. ' +
        'Please do so via: client.use(:db_name).'

      # Instantiate the new exception.
      #
      # @example Instantiate the exception.
      #   NoDatabase.new
      #
      # @since 2.0.0
      def initialize
        super(MESSAGE)
      end
    end

    class << self

      # Gets a new client given the provided uri connection string.
      #
      # @example Get a client from the connection string.
      #   Mongo::Client.connect("mongodb://127.0.0.1:27017/testdb?w=3")
      #
      # @param [ String ] connection_string The connection string.
      #
      # @see http://docs.mongodb.org/manual/reference/connection-string/
      #
      # @since 2.0.0
      def connect(connection_string)
        uri = URI.new(connection_string)
        client = new(uri.nodes, uri.options)
        database = uri.database
        client.use(database) if database
        client
      end
    end

    private

    # Get the current database that the client is operating on.
    #
    # @api private
    #
    # @example Get the current database.
    #   client.database
    #
    # @raise [ Mongo::NoDatabase ] If no database has been set.
    #
    # @return [ Mongo::Database ] The current database.
    #
    # @since 2.0.0
    def database
      @database || raise(NoDatabase.new)
    end
  end
end

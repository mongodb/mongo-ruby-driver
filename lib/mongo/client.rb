# Copyright (C) 2013 10gen Inc.
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

module Mongo

  # The client is the entry point to the driver and is the main object that
  # will be interacted with.
  #
  # @since 2.0.0
  class Client

    # @!attribute cluster
    #   @return [ Mongo::Cluster ] The cluster of nodes for the client.
    # @!attribute options
    #   @return [ Hash ] The configuration options.
    attr_reader :cluster, :options

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
      @database = Database.new(database_name)
    end

    # Exception that is raised when trying to perform operations before ever
    # telling the client which database to execute ops on.
    #
    # @since 2.0.0
    class NoDatabase < RuntimeError

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

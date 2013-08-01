# Copyright (C) 2013 10gen Inc.
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

  # Represents a database on the db server and operations that can execute on
  # it at this level.
  #
  # @since 2.0.0
  class Database

    # The "collection" that database commands operate against.
    #
    # @since 2.0.0
    COMMAND = '$cmd'.freeze

    # @return [ Mongo::Client ] The database client.
    attr_reader :client
    # @return [ String ] The name of the collection.
    attr_reader :name

    # Get a collection in this database by the provided name.
    #
    # @example Get a collection.
    #   database[:users]
    #
    # @param [ String, Symbol ] collection_name The name of the collection.
    #
    # @return [ Mongo::Collection ] The collection object.
    #
    # @since 2.0.0
    def [](collection_name)
      Collection.new(self, collection_name)
    end

    # Execute a command on the database.
    #
    # @example Execute a command.
    #   database.command(:ismaster => 1)
    #
    # @param [ Hash ] operation The command to execute.
    #
    # @return [ Hash ] The result of the command execution.
    def command(operation)
      cmd = Protocol::Query.new(
        name,
        COMMAND,
        operation,
        :limit => -1, :read => client.read_preference
      )
      cluster.execute(cmd)
    end

    # Instantiate a new database object.
    #
    # @example Instantiate the database.
    #   Mongo::Database.new(client, :test)
    #
    # @param [ Mongo::Client ] client The driver client.
    # @param [ String, Symbol ] name The name of the database.
    #
    # @raise [ Mongo::Database::InvalidName ] If the name is nil.
    #
    # @since 2.0.0
    def initialize(client, name)
      raise InvalidName.new unless name
      @client = client
      @name = name.to_s
    end

    # Exception that is raised when trying to create a database with no name.
    #
    # @since 2.0.0
    class InvalidName < RuntimeError

      # The message is constant.
      #
      # @since 2.0.0
      MESSAGE = 'nil is an invalid database name. ' +
        'Please provide a string or symbol.'

      # Instantiate the new exception.
      #
      # @example Instantiate the exception.
      #   Mongo::Database::InvalidName.new
      #
      # @since 2.0.0
      def initialize
        super(MESSAGE)
      end
    end

    private

    # Get the cluster from the client to execute operations on.
    #
    # @api private
    #
    # @example Get the cluster.
    #   database.cluster
    #
    # @return [ Mongo::Cluster ] The cluster.
    #
    # @since 2.0.0
    def cluster
      client.cluster
    end
  end
end

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

module Mongo

  # Represents a database on the db server and operations that can execute on
  # it at this level.
  #
  # @since 2.0.0
  class Database
    extend Forwardable

    # The admin database name.
    #
    # @since 2.0.0
    ADMIN = 'admin'.freeze

    # The "collection" that database commands operate against.
    #
    # @since 2.0.0
    COMMAND = '$cmd'.freeze

    # The name of the collection that holds all the collection names.
    #
    # @since 2.0.0
    NAMESPACES = 'system.namespaces'.freeze

    # @return [ Mongo::Client ] The database client.
    attr_reader :client

    # @return [ String ] The name of the collection.
    attr_reader :name

    # Get cluser and server preference from client.
    def_delegators :@client, :cluster, :write_concern

    # Check equality of the database object against another. Will simply check
    # if the names are the same.
    #
    # @example Check database equality.
    #   database == other
    #
    # @param [ Object ] other The object to check against.
    #
    # @return [ true, false ] If the objects are equal.
    #
    # @since 2.0.0
    def ==(other)
      return false unless other.is_a?(Database)
      name == other.name
    end

    # Get the server (read) preference from the options passed to this database
    #
    # @param [ Hash ] opts Options passed to an operation.
    #
    # @option opts [ Symbol ] :read Read preference.
    #
    # @return [ ServerPreference ] the server preference for this db or operation.
    #
    # @since 2.0.0
    def server_preference(opts={})
      return ServerPreference.get(:mode => opts[:read]) if opts[:read]
      @server_preference || client.server_preference
    end

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
    alias_method :collection, :[]

    # Get all the names of the non system collections in the database.
    #
    # @example Get the collection names.
    #   database.collection_names
    #
    # @return [ Array<String> ] The names of all non-system collections.
    #
    # @since 2.0.0
    def collection_names
      namespaces = collection(NAMESPACES).find(
        :name => { '$not' => /#{name}\.system\,|\$/ }
      )
      namespaces.map do |document|
        collection = document['name']
        collection[name.length + 1, collection.length]
      end
    end

    # Get all the collections that belong to this database.
    #
    # @example Get all the collections.
    #   database.collections
    #
    # @return [ Array<Mongo::Collection> ] All the collections.
    #
    # @since 2.0.0
    def collections
      collection_names.map { |name| collection(name) }
    end

    # Execute a command on the database.
    #
    # @example Execute a command.
    #   database.command(:ismaster => 1)
    #
    # @param [ Hash ] operation The command to execute.
    # @param [ Hash ] opts Options for this command.
    #
    # @option opts [ Symbol ] :read Read preference for this operation.
    # @option opts [ String ] :db_name (this db) The database against which to run
    # @option opts [ Context ] :context A context with which to execute the command.
    #  the command.
    #
    # @return [ Hash ] The result of the command execution.
    def command(operation, opts={})
      Operation::Command.new({
        :selector => operation,
        :db_name => opts[:db_name] || name,
        :opts => { :limit => -1 }
      }).execute(get_context(opts)).documents[0]
    end

    # Drops the entire collection from the database.
    # USE WITH CAUTION, THIS CANNOT BE UNDONE.
    #
    # @param [ String ] collection The collection name.
    #
    # @since 2.0.0
    def drop_collection(name)
      command({ :drop => name })
    end

    # Rename a collection.
    #
    # @note If operating in auth mode, the client must be authorized as an admin to
    #  perform this operation.
    #
    # @param [ String ] oldname The current name of the collection.
    # @param [ String ] newname The new desired collection name.
    # @param [ true, false ] drop (true) If true, and there is already a collection
    #  with the name 'newname', drop that collection first.  If drop is false and such
    #  a collection exists, an error will be raised.
    #
    # @since 2.0.0
    def rename_collection(oldname, newname, drop=true)
      Collection.validate_name(newname)
      res = command({ :renameCollection => "#{name}.#{oldname}",
                      :to               => "#{name}.#{newname}",
                      :dropTarget       => drop },
                    { :db_name => ADMIN })
      # @todo - process differently once command response objects are done.
      if res['ok'] != 1
        raise Mongo::OperationError, "Error naming collection: #{res.inspect}"
      end
    end

    # Instantiate a new database object.
    #
    # @example Instantiate the database.
    #   Mongo::Database.new(client, :test)
    #
    # @param [ Mongo::Client ] client The driver client.
    # @param [ String, Symbol ] name The name of the database.
    # @param [ Hash ] opts Options for this database.
    #
    # @option opts [ Symbol ] :read Read preference.
    #
    # @raise [ Mongo::Database::InvalidName ] If the name is nil.
    #
    # @since 2.0.0
    def initialize(client, name, opts={})
      raise InvalidName.new unless name
      @client = client
      @name = name.to_s
      @server_preference = ServerPreference.get(:mode => opts[:read]) if opts[:read]
    end

    # Exception that is raised when trying to create a database with no name.
    #
    # @since 2.0.0
    class InvalidName < DriverError

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

    # Get a server context for this operation.
    #
    # @param [ Hash ] opts Options from the query.
    #
    # @return [ Context ] a context object.
    #
    # @since 2.0.0
    def get_context(opts)
      return opts[:context] if opts[:context]
      server_preference(opts).select_servers(cluster.servers).first.context
    end
  end
end

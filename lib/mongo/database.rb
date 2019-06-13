# Copyright (C) 2014-2019 MongoDB, Inc.
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

require 'mongo/database/view'

module Mongo

  # Represents a database on the db server and operations that can execute on
  # it at this level.
  #
  # @since 2.0.0
  class Database
    extend Forwardable
    include Retryable

    # The admin database name.
    #
    # @since 2.0.0
    ADMIN = 'admin'.freeze

    # The "collection" that database commands operate against.
    #
    # @since 2.0.0
    COMMAND = '$cmd'.freeze

    # The default database options.
    #
    # @since 2.0.0
    DEFAULT_OPTIONS = Options::Redacted.new(:database => ADMIN).freeze

    # Database name field constant.
    #
    # @since 2.1.0
    NAME = 'name'.freeze

    # Databases constant.
    #
    # @since 2.1.0
    DATABASES = 'databases'.freeze

    # The name of the collection that holds all the collection names.
    #
    # @since 2.0.0
    NAMESPACES = 'system.namespaces'.freeze

    # @return [ Client ] client The database client.
    attr_reader :client

    # @return [ String ] name The name of the database.
    attr_reader :name

    # @return [ Hash ] options The options.
    attr_reader :options

    # Get cluster, read preference, and write concern from client.
    def_delegators :@client,
                   :cluster,
                   :read_preference,
                   :server_selector,
                   :read_concern,
                   :write_concern

    # @return [ Mongo::Server ] Get the primary server from the cluster.
    def_delegators :cluster,
                   :next_primary

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

    # Get a collection in this database by the provided name.
    #
    # @example Get a collection.
    #   database[:users]
    #
    # @param [ String, Symbol ] collection_name The name of the collection.
    # @param [ Hash ] options The options to the collection.
    #
    # @return [ Mongo::Collection ] The collection object.
    #
    # @since 2.0.0
    def [](collection_name, options = {})
      Collection.new(self, collection_name, options)
    end
    alias_method :collection, :[]

    # Get all the names of the non-system collections in the database.
    #
    # @example Get the collection names.
    #   database.collection_names
    #
    # @return [ Array<String> ] The names of all non-system collections.
    #
    # @since 2.0.0
    def collection_names(options = {})
      View.new(self).collection_names(options)
    end

    # Get info on all the collections in the database.
    #
    # @example Get info on each collection.
    #   database.list_collections
    #
    # @return [ Array<Hash> ] Info for each collection in the database.
    #
    # @since 2.0.5
    def list_collections
      View.new(self).list_collections
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
    # @param [ Hash ] opts The command options.
    #
    # @option opts :read [ Hash ] The read preference for this command.
    # @option opts :session [ Session ] The session to use for this command.
    #
    # @return [ Hash ] The result of the command execution.
    def command(operation, opts = {})
      txn_read_pref = if opts[:session] && opts[:session].in_transaction?
        opts[:session].txn_read_preference
      else
        nil
      end
      txn_read_pref ||= opts[:read] || ServerSelector::PRIMARY
      Lint.validate_underscore_read_preference(txn_read_pref)
      preference = ServerSelector.get(txn_read_pref)

      client.send(:with_session, opts) do |session|
        read_with_retry(session, preference) do |server|
          Operation::Command.new({
            :selector => operation.dup,
            :db_name => name,
            :read => preference,
            :session => session
          }).execute(server)
        end
      end
    end

    # Drop the database and all its associated information.
    #
    # @example Drop the database.
    #   database.drop
    #
    # @param [ Hash ] options The options for the operation.
    #
    # @option options [ Session ] :session The session to use for the operation.
    #
    # @return [ Result ] The result of the command.
    #
    # @since 2.0.0
    def drop(options = {})
      operation = { :dropDatabase => 1 }
      client.send(:with_session, options) do |session|
        Operation::DropDatabase.new({
          selector: operation,
          db_name: name,
          write_concern: write_concern,
          session: session
        }).execute(next_primary(nil, session))
      end
    end

    # Instantiate a new database object.
    #
    # @example Instantiate the database.
    #   Mongo::Database.new(client, :test)
    #
    # @param [ Mongo::Client ] client The driver client.
    # @param [ String, Symbol ] name The name of the database.
    # @param [ Hash ] options The options.
    #
    # @raise [ Mongo::Database::InvalidName ] If the name is nil.
    #
    # @since 2.0.0
    def initialize(client, name, options = {})
      raise Error::InvalidDatabaseName.new unless name
      @client = client
      @name = name.to_s.freeze
      @options = options.freeze
    end

    # Get a pretty printed string inspection for the database.
    #
    # @example Inspect the database.
    #   database.inspect
    #
    # @return [ String ] The database inspection.
    #
    # @since 2.0.0
    def inspect
      "#<Mongo::Database:0x#{object_id} name=#{name}>"
    end

    # Get the Grid "filesystem" for this database.
    #
    # @example Get the GridFS.
    #   database.fs
    #
    # @return [ Grid::FSBucket ] The GridFS for the database.
    #
    # @since 2.0.0
    def fs(options = {})
      Grid::FSBucket.new(self, options)
    end

    # Get the user view for this database.
    #
    # @example Get the user view.
    #   database.users
    #
    # @return [ View::User ] The user view.
    #
    # @since 2.0.0
    def users
      Auth::User::View.new(self)
    end

    # As of version 3.6 of the MongoDB server, a ``$changeStream`` pipeline stage is supported
    # in the aggregation framework. As of version 4.0, this stage allows users to request that
    # notifications are sent for all changes that occur in the client's database.
    #
    # @example Get change notifications for a given database..
    #  database.watch([{ '$match' => { operationType: { '$in' => ['insert', 'replace'] } } }])
    #
    # @param [ Array<Hash> ] pipeline Optional additional filter operators.
    # @param [ Hash ] options The change stream options.
    #
    # @option options [ String ] :full_document Allowed values: 'default', 'updateLookup'.
    #   Defaults to 'default'. When set to 'updateLookup', the change notification for partial
    #   updates will include both a delta describing the changes to the document, as well as a copy
    #   of the entire document that was changed from some time after the change occurred.
    # @option options [ BSON::Document, Hash ] :resume_after Specifies the logical starting point
    #   for the new change stream.
    # @option options [ Integer ] :max_await_time_ms The maximum amount of time for the server to
    #   wait on new documents to satisfy a change stream query.
    # @option options [ Integer ] :batch_size The number of documents to return per batch.
    # @option options [ BSON::Document, Hash ] :collation The collation to use.
    # @option options [ Session ] :session The session to use.
    # @option options [ BSON::Timestamp ] :start_at_operation_time Only return
    #   changes that occurred after the specified timestamp. Any command run
    #   against the server will return a cluster time that can be used here.
    #   Only recognized by server versions 4.0+.
    #
    # @note A change stream only allows 'majority' read concern.
    # @note This helper method is preferable to running a raw aggregation with a $changeStream
    #   stage, for the purpose of supporting resumability.
    #
    # @return [ ChangeStream ] The change stream object.
    #
    # @since 2.6.0
    def watch(pipeline = [], options = {})
      Mongo::Collection::View::ChangeStream.new(
        Mongo::Collection::View.new(collection("#{COMMAND}.aggregate")),
        pipeline,
        Mongo::Collection::View::ChangeStream::DATABASE,
        options)
    end

    # Create a database for the provided client, for use when we don't want the
    # client's original database instance to be the same.
    #
    # @api private
    #
    # @example Create a database for the client.
    #   Database.create(client)
    #
    # @param [ Client ] client The client to create on.
    #
    # @return [ Database ] The database.
    #
    # @since 2.0.0
    def self.create(client)
      database = Database.new(client, client.options[:database], client.options)
      client.instance_variable_set(:@database, database)
    end
  end
end

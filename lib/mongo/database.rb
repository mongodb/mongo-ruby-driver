# frozen_string_literal: true

# Copyright (C) 2014-2020 MongoDB Inc.
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
require 'mongo/database/cursor_command_view'

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
    ADMIN = 'admin'

    # The "collection" that database commands operate against.
    #
    # @since 2.0.0
    COMMAND = '$cmd'

    # The default database options.
    #
    # @since 2.0.0
    DEFAULT_OPTIONS = Options::Redacted.new(database: ADMIN).freeze

    # Database name field constant.
    #
    # @since 2.1.0
    # @deprecated
    NAME = 'name'

    # Databases constant.
    #
    # @since 2.1.0
    DATABASES = 'databases'

    # The name of the collection that holds all the collection names.
    #
    # @since 2.0.0
    NAMESPACES = 'system.namespaces'

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
                   :write_concern,
                   :encrypted_fields_map,
                   :tracer

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
      if options[:server_api]
        raise ArgumentError,
              'The :server_api option cannot be specified for collection objects. It can only be specified on Client level'
      end

      Collection.new(self, collection_name, options)
    end
    alias collection []

    # Get all the names of the non-system collections in the database.
    #
    # @note The set of returned collection names depends on the version of
    #   MongoDB server that fulfills the request.
    #
    # @param [ Hash ] options
    #
    # @option options [ Hash ] :filter A filter on the collections returned.
    # @option options [ true, false ] :authorized_collections A flag, when
    #   set to true and used with nameOnly: true, that allows a user without the
    #   required privilege to run the command when access control is enforced
    # @option options [ Object ] :comment A user-provided
    #   comment to attach to this command.
    # @option options [ Integer ] :timeout_ms The operation timeout in milliseconds.
    #    Must be a non-negative integer. An explicit value of 0 means infinite.
    #    The default value is unset which means the value is inherited from
    #    the database or the client.
    #
    #   See https://mongodb.com/docs/manual/reference/command/listCollections/
    #   for more information and usage.
    #
    # @return [ Array<String> ] Names of the collections.
    #
    # @since 2.0.0
    def collection_names(options = {})
      View.new(self, options).collection_names(options)
    end

    # Get info on all the non-system collections in the database.
    #
    # @note The set of collections returned, and the schema of the
    #   information hash per collection, depends on the MongoDB server
    #   version that fulfills the request.
    #
    # @param [ Hash ] options
    #
    # @option options [ Hash ] :filter A filter on the collections returned.
    # @option options [ true, false ] :name_only Indicates whether command
    #   should return just collection/view names and type or return both the
    #   name and other information
    # @option options [ true, false ] :authorized_collections A flag, when
    #   set to true and used with nameOnly: true, that allows a user without the
    #   required privilege to run the command when access control is enforced.
    # @option options [ Object ] :comment A user-provided
    #   comment to attach to this command.
    # @option options [ Integer ] :timeout_ms The operation timeout in milliseconds.
    #    Must be a non-negative integer. An explicit value of 0 means infinite.
    #    The default value is unset which means the value is inherited from
    #    the database or the client.
    #
    #   See https://mongodb.com/docs/manual/reference/command/listCollections/
    #   for more information and usage.
    #
    # @return [ Array<Hash> ] Array of information hashes, one for each
    #   collection in the database.
    #
    # @since 2.0.5
    def list_collections(options = {})
      View.new(self, options).list_collections(options)
    end

    # Get all the non-system collections that belong to this database.
    #
    # @note The set of returned collections depends on the version of
    #   MongoDB server that fulfills the request.
    #
    # @param [ Hash ] options
    #
    # @option options [ Hash ] :filter A filter on the collections returned.
    # @option options [ true, false ] :authorized_collections A flag, when
    #   set to true and used with name_only: true, that allows a user without the
    #   required privilege to run the command when access control is enforced.
    # @option options [ Object ] :comment A user-provided
    #   comment to attach to this command.
    # @option options [ Integer ] :timeout_ms The operation timeout in milliseconds.
    #    Must be a non-negative integer. An explicit value of 0 means infinite.
    #    The default value is unset which means the value is inherited from
    #    the database or the client.
    #
    #   See https://mongodb.com/docs/manual/reference/command/listCollections/
    #   for more information and usage.
    #
    # @return [ Array<Mongo::Collection> ] The collections.
    #
    # @since 2.0.0
    def collections(options = {})
      collection_names(options).map { |name| collection(name) }
    end

    # Execute a command on the database.
    #
    # @example Execute a command.
    #   database.command(:hello => 1)
    #
    # @param [ Hash ] operation The command to execute.
    # @param [ Hash ] opts The command options.
    #
    # @option opts :read [ Hash ] The read preference for this command.
    # @option opts :session [ Session ] The session to use for this command.
    # @option options [ Integer ] :timeout_ms The operation timeout in milliseconds.
    #    Must be a non-negative integer. An explicit value of 0 means infinite.
    #    The default value is unset which means the value is inherited from
    #    the database or the client.
    # @option opts :execution_options [ Hash ] Options to pass to the code that
    #   executes this command. This is an internal option and is subject to
    #   change.
    #   - :deserialize_as_bson [ Boolean ] Whether to deserialize the response
    #     to this command using BSON types instead of native Ruby types wherever
    #     possible.
    #
    # @return [ Mongo::Operation::Result ] The result of the command execution.
    def command(operation, opts = {})
      opts = opts.dup
      execution_opts = opts.delete(:execution_options) || {}

      txn_read_pref = (opts[:session].txn_read_preference if opts[:session] && opts[:session].in_transaction?)
      txn_read_pref ||= opts[:read] || ServerSelector::PRIMARY
      Lint.validate_underscore_read_preference(txn_read_pref)
      selector = ServerSelector.get(txn_read_pref)

      client.with_session(opts) do |session|
        context = Operation::Context.new(
          client: client,
          session: session,
          operation_timeouts: operation_timeouts(opts)
        )
        op = Operation::Command.new(
          selector: operation,
          db_name: name,
          read: selector,
          session: session
        )

        retry_enabled = client.options[:retry_reads] != false &&
                        client.options[:retry_writes] != false
        with_overload_retry(context: context, retry_enabled: retry_enabled) do
          server = selector.select_server(cluster, nil, session)
          op.execute(server, context: context, options: execution_opts)
        end
      end
    end

    # Run a command that returns a cursor and parse the response as a cursor.
    #
    # The command is sent to the server unmodified; the driver MUST NOT inspect
    # or alter it. If the response does not contain a cursor field an error is
    # raised. The command is never retried.
    #
    # Note: if a +maxTimeMS+ field is already set on the command document it is
    # left as-is. The +max_time_ms+ option below applies only to getMore
    # commands. Setting both +timeout_ms+ and +max_time_ms+ is not supported
    # and has undefined behavior.
    #
    # @example Run a cursor-returning command.
    #   database.cursor_command(checkMetadataConsistency: 1)
    #
    # @param [ Hash ] command The command to execute.
    # @param [ Hash ] options The command options.
    #
    # @option options [ Hash ] :read The read preference for this command,
    #   used for server selection and reused for subsequent getMores.
    # @option options [ Session ] :session The session to use. If none is
    #   given an implicit session is created and reused for the cursor's
    #   lifetime.
    # @option options [ Integer ] :timeout_ms The operation timeout in
    #   milliseconds.
    # @option options [ Integer ] :batch_size The batchSize to send on getMore
    #   commands.
    # @option options [ Integer ] :max_time_ms The maxTimeMS to send on getMore
    #   commands.
    # @option options [ Object ] :comment A comment to attach to getMore
    #   commands.
    # @option options [ Symbol ] :cursor_type The cursor type, :tailable or
    #   :tailable_await. Must match the flags set on the command document.
    # @option options [ Symbol ] :timeout_mode :cursor_lifetime or :iteration.
    #
    # @return [ Mongo::Cursor ] A cursor over the command results.
    #
    # @raise [ Error::InvalidCursorOperation ] If the response does not contain
    #   a cursor.
    def cursor_command(command, options = {})
      options = options.dup
      execution_opts = options.delete(:execution_options) || {}
      view_options = extract_cursor_command_view_options(options)

      txn_read_pref = (options[:session].txn_read_preference if options[:session] && options[:session].in_transaction?)
      txn_read_pref ||= options[:read] || ServerSelector::PRIMARY
      Lint.validate_underscore_read_preference(txn_read_pref)
      selector = ServerSelector.get(txn_read_pref)

      # The session is intentionally not wrapped in #with_session: an implicit
      # session must outlive this method and is ended by the cursor when it is
      # exhausted or closed. Until the cursor takes ownership, the session and
      # any load-balanced connection are cleaned up here on every exit path.
      session = client.get_session(options)
      context = Operation::Context.new(
        client: client,
        session: session,
        operation_timeouts: operation_timeouts(options)
      )
      op = Operation::CursorCommand.new(
        selector: command,
        db_name: name,
        read: selector,
        session: session
      )

      # Per the client-backpressure spec, retrying a generic command on
      # overload errors requires both retryable reads and writes to be
      # enabled, same as Database#command.
      retry_enabled = client.options[:retry_reads] != false &&
                      client.options[:retry_writes] != false

      server = nil
      connection = nil
      cursor = nil
      begin
        result = with_overload_retry(context: context, retry_enabled: retry_enabled) do
          server = selector.select_server(cluster, nil, session)
          if server.load_balancer?
            # The connection is checked in by the cursor when it is drained.
            connection = check_out_cursor_command_connection(server, context)
            begin
              op.execute_with_connection(connection, context: context, options: execution_opts)
            rescue StandardError
              # Release the connection before the error propagates so that
              # a retried attempt checks out a fresh one.
              connection.connection_pool.check_in(connection) unless connection.pinned?
              connection = nil
              raise
            end
          else
            op.execute(server, context: context, options: execution_opts)
          end
        end

        unless result.cursor?
          raise Error::InvalidCursorOperation,
                'The command response did not include a cursor. ' \
                'Use Database#command for commands that do not return a cursor.'
        end

        view = CursorCommandView.new(self, view_options)
        cursor = Cursor.new(view, result, server, session: session, context: context)
      ensure
        # If the cursor was created it owns the session and connection;
        # otherwise (error or no cursor in the response) release them here.
        unless cursor
          connection.connection_pool.check_in(connection) if connection && !connection.pinned?
          session.end_session if session && session.implicit?
        end
      end
      cursor
    end

    # Execute a read command on the database, retrying the read if necessary.
    #
    # @param [ Hash ] operation The command to execute.
    # @param [ Hash ] opts The command options.
    #
    # @option opts :read [ Hash ] The read preference for this command.
    # @option opts :session [ Session ] The session to use for this command.
    # @option opts [ Object ] :comment A user-provided
    #   comment to attach to this command.
    # @option opts [ Integer ] :timeout_ms The operation timeout in milliseconds.
    #    Must be a non-negative integer. An explicit value of 0 means infinite.
    #    The default value is unset which means the value is inherited from
    #    the database or the client.
    # @option opts :op_name [ String | nil ] The name of the operation for
    #    tracing purposes.
    #
    # @return [ Hash ] The result of the command execution.
    # @api private
    def read_command(operation, opts = {})
      txn_read_pref = (opts[:session].txn_read_preference if opts[:session] && opts[:session].in_transaction?)
      txn_read_pref ||= opts[:read] || ServerSelector::PRIMARY
      Lint.validate_underscore_read_preference(txn_read_pref)
      preference = ServerSelector.get(txn_read_pref)

      client.with_session(opts) do |session|
        context = Operation::Context.new(
          client: client,
          session: session,
          operation_timeouts: operation_timeouts(opts)
        )
        operation = Operation::Command.new(
          selector: operation.dup,
          db_name: name,
          read: preference,
          session: session,
          comment: opts[:comment]
        )
        op_name = opts[:op_name] || 'command'
        tracer.trace_operation(operation, context, op_name: op_name) do
          read_with_retry(session, preference, context) do |server|
            operation.execute(server, context: context)
          end
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
    # @option options [ Hash ] :write_concern The write concern options.
    # @option options [ Integer ] :timeout_ms The operation timeout in milliseconds.
    #    Must be a non-negative integer. An explicit value of 0 means infinite.
    #    The default value is unset which means the value is inherited from
    #    the database or the client.
    #
    # @return [ Result ] The result of the command.
    #
    # @since 2.0.0
    def drop(options = {})
      operation = { dropDatabase: 1 }
      client.with_session(options) do |session|
        write_concern = if options[:write_concern]
                          WriteConcern.get(options[:write_concern])
                        else
                          self.write_concern
                        end
        Operation::DropDatabase.new({
                                      selector: operation,
                                      db_name: name,
                                      write_concern: write_concern,
                                      session: session
                                    }).execute(
                                      next_primary(nil, session),
                                      context: Operation::Context.new(
                                        client: client,
                                        session: session,
                                        operation_timeouts: operation_timeouts(options)
                                      )
                                    )
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
    # @option options [ Integer ] :timeout_ms The operation timeout in milliseconds.
    #    Must be a non-negative integer. An explicit value of 0 means infinite.
    #    The default value is unset which means the value is inherited from
    #    the client.
    #
    # @raise [ Mongo::Database::InvalidName ] If the name is nil.
    #
    # @since 2.0.0
    def initialize(client, name, options = {})
      raise Error::InvalidDatabaseName.new unless name
      if Lint.enabled? && !(name.is_a?(String) || name.is_a?(Symbol))
        raise "Database name must be a string or a symbol: #{name}"
      end

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
    # @param [ Hash ] options The GridFS options.
    #
    # @option options [ String ] :bucket_name The prefix for the files and chunks
    #   collections.
    # @option options [ Integer ] :chunk_size Override the default chunk
    #   size.
    # @option options [ String ] :fs_name The prefix for the files and chunks
    #   collections.
    # @option options [ String ] :read The read preference.
    # @option options [ Session ] :session The session to use.
    # @option options [ Hash ] :write Deprecated. Equivalent to :write_concern
    #   option.
    # @option options [ Hash ] :write_concern The write concern options.
    #   Can be :w => Integer|String, :fsync => Boolean, :j => Boolean.
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

    # Perform an aggregation on the database.
    #
    # @example Perform an aggregation.
    #   collection.aggregate([ { "$listLocalSessions" => {} } ])
    #
    # @param [ Array<Hash> ] pipeline The aggregation pipeline.
    # @param [ Hash ] options The aggregation options.
    #
    # @option options [ true, false ] :allow_disk_use Set to true if disk
    #   usage is allowed during the aggregation.
    # @option options [ Integer ] :batch_size The number of documents to return
    #   per batch.
    # @option options [ true, false ] :bypass_document_validation Whether or
    #   not to skip document level validation.
    # @option options [ Hash ] :collation The collation to use.
    # @option options [ Object ] :comment A user-provided
    #   comment to attach to this command.
    # @option options [ Integer ] :max_time_ms The maximum amount of time to
    #   allow the query to run, in milliseconds. This option is deprecated, use
    #   :timeout_ms instead.
    # @option options [ Integer ] :timeout_ms The operation timeout in milliseconds.
    #    Must be a non-negative integer. An explicit value of 0 means infinite.
    #    The default value is unset which means the value is inherited from
    #    the database or the client.
    # @option options [ String ] :hint The index to use for the aggregation.
    # @option options [ Session ] :session The session to use.
    #
    # @return [ Collection::View::Aggregation ] The aggregation object.
    #
    # @since 2.10.0
    def aggregate(pipeline, options = {})
      View.new(self, options).aggregate(pipeline, options)
    end

    # Allows users to request that notifications are sent for all changes that
    # occur in the client's database.
    #
    # @example Get change notifications for a given database..
    #  database.watch([{ '$match' => { operationType: { '$in' => ['insert', 'replace'] } } }])
    #
    # @param [ Array<Hash> ] pipeline Optional additional filter operators.
    # @param [ Hash ] options The change stream options.
    #
    # @option options [ String ] :full_document Allowed values: nil, 'default',
    #   'updateLookup', 'whenAvailable', 'required'.
    #
    #   The default is to not send a value (i.e. nil), which is equivalent to
    #   'default'. By default, the change notification for partial updates will
    #   include a delta describing the changes to the document.
    #
    #   When set to 'updateLookup', the change notification for partial updates
    #   will include both a delta describing the changes to the document as well
    #   as a copy of the entire document that was changed from some time after
    #   the change occurred.
    #
    #   When set to 'whenAvailable', configures the change stream to return the
    #   post-image of the modified document for replace and update change events
    #   if the post-image for this event is available.
    #
    #   When set to 'required', the same behavior as 'whenAvailable' except that
    #   an error is raised if the post-image is not available.
    # @option options [ String ] :full_document_before_change Allowed values: nil,
    #   'whenAvailable', 'required', 'off'.
    #
    #   The default is to not send a value (i.e. nil), which is equivalent to 'off'.
    #
    #   When set to 'whenAvailable', configures the change stream to return the
    #   pre-image of the modified document for replace, update, and delete change
    #   events if it is available.
    #
    #   When set to 'required', the same behavior as 'whenAvailable' except that
    #   an error is raised if the pre-image is not available.
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
    # @option options [ Object ] :comment A user-provided
    #   comment to attach to this command.
    # @option options [ Boolean ] :show_expanded_events Enables the server to
    #   send the 'expanded' list of change stream events. The list of additional
    #   events included with this flag set are: createIndexes, dropIndexes,
    #   modify, create, shardCollection, reshardCollection,
    #   refineCollectionShardKey.
    #
    # @note A change stream only allows 'majority' read concern.
    # @note This helper method is preferable to running a raw aggregation with a $changeStream
    #   stage, for the purpose of supporting resumability.
    #
    # @return [ ChangeStream ] The change stream object.
    #
    # @since 2.6.0
    def watch(pipeline = [], options = {})
      view_options = options.dup
      view_options[:cursor_type] = :tailable_await if options[:max_await_time_ms]

      Mongo::Collection::View::ChangeStream.new(
        Mongo::Collection::View.new(collection("#{COMMAND}.aggregate"), {}, view_options),
        pipeline,
        Mongo::Collection::View::ChangeStream::DATABASE,
        options
      )
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

    # @return [ Integer | nil ] Operation timeout that is for this database or
    #   for the corresponding client.
    #
    # @api private
    def timeout_ms
      options[:timeout_ms] || client.timeout_ms
    end

    # @return [ Hash ] timeout_ms value set on the operation level (if any),
    #   and/or timeout_ms that is set on collection/database/client level (if any).
    #
    # @api private
    def operation_timeouts(opts)
      # TODO: We should re-evaluate if we need two timeouts separately.
      {}.tap do |result|
        if opts[:timeout_ms].nil?
          result[:inherited_timeout_ms] = timeout_ms
        else
          result[:operation_timeout_ms] = opts.delete(:timeout_ms)
        end
      end
    end

    private

    # Removes the getMore and cursor options from the options hash and returns
    # them as a separate hash for the CursorCommandView. The remaining options
    # (e.g. :session, :read, :timeout_ms) are left for command execution.
    #
    # @param [ Hash ] options The cursor_command options (mutated).
    #
    # @return [ Hash ] The view options.
    def extract_cursor_command_view_options(options)
      %i[ batch_size max_time_ms comment cursor_type timeout_mode ].each_with_object({}) do |key, view_options|
        view_options[key] = options.delete(key) if options.key?(key)
      end
    end

    # Checks out a load balanced connection for a cursor command. If the
    # session is pinned to a connection (e.g. in a transaction), that
    # connection is reused.
    #
    # @param [ Server ] server The load balancer server.
    # @param [ Operation::Context ] context The operation context.
    #
    # @return [ Server::Connection ] The checked out connection.
    def check_out_cursor_command_connection(server, context)
      connection = if context.connection_global_id
                     server.pool.check_out_pinned_connection(context.connection_global_id)
                   end
      connection || server.pool.check_out(context: context)
    end
  end
end

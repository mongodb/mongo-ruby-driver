# frozen_string_literal: true
# rubocop:todo all

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

require 'mongo/cursor/nontailable'

module Mongo
  class Database

    # A class representing a view of a database.
    #
    # @since 2.0.0
    class View
      extend Forwardable
      include Enumerable
      include Retryable
      include Mongo::CursorHost
      include Cursor::NonTailable

      def_delegators :@database, :cluster, :read_preference, :client
      # @api private
      def_delegators :@database, :server_selector, :read_concern, :write_concern
      def_delegators :cluster, :next_primary

      # @return [ Integer ] batch_size The size of the batch of results
      #   when sending the listCollections command.
      attr_reader :batch_size

      # @return [ Integer ] limit The limit when sending a command.
      attr_reader :limit

      # @return [ Collection ] collection The command collection.
      attr_reader :collection

      # Get all the names of the non-system collections in the database.
      #
      # @note The set of returned collection names depends on the version of
      #   MongoDB server that fulfills the request.
      #
      # @param [ Hash ] options Options for the listCollections command.
      #
      # @option options [ Integer ] :batch_size  The batch size for results
      #   returned from the listCollections command.
      # @option options [ Hash ] :filter A filter on the collections returned.
      # @option options [ true, false ] :authorized_collections A flag, when
      #   set to true, that allows a user without the required privilege
      #   to run the command when access control is enforced.
      # @option options [ Object ] :comment A user-provided
      #   comment to attach to this command.
      # @option options [ Integer ] :timeout_ms The operation timeout in milliseconds.
      #    Must be a non-negative integer. An explicit value of 0 means infinite.
      #    The default value is unset which means the value is inherited from
      #    the database or the client.
      #
      #   See https://mongodb.com/docs/manual/reference/command/listCollections/
      #   for more information and usage.
      # @option options [ Session ] :session The session to use.
      #
      # @return [ Array<String> ] The names of all non-system collections.
      #
      # @since 2.0.0
      def collection_names(options = {})
        @batch_size = options[:batch_size]
        session = client.get_session(options)
        context = Operation::Context.new(
          client: client,
          session: session,
          operation_timeouts: operation_timeouts(options)
        )
        cursor = read_with_retry_cursor(session, ServerSelector.primary, self, context: context) do |server|
          send_initial_query(server, session, context, options.merge(name_only: true))
        end
        cursor.map do |info|
          if cursor.initial_result.connection_description.features.list_collections_enabled?
            info['name']
          else
            (info['name'] &&
              info['name'].sub("#{@database.name}.", ''))
          end
        end.reject do |name|
          name.start_with?('system.') || name.include?('$')
        end
      end

      # Get info on all the collections in the database.
      #
      # @note The set of collections returned, and the schema of the
      #   information hash per collection, depends on the MongoDB server
      #   version that fulfills the request.
      #
      # @example Get info on each collection.
      #   database.list_collections
      #
      # @param [ Hash ] options
      #
      # @option options [ Hash ] :filter A filter on the collections returned.
      # @option options [ true, false ] :name_only Indicates whether command
      #   should return just collection/view names and type or return both the
      #   name and other information
      # @option options [ true, false ] :authorized_collections A flag, when
      #   set to true and used with nameOnly: true, that allows a user without the
      #   required privilege to run the command when access control is enforced
      #
      #   See https://mongodb.com/docs/manual/reference/command/listCollections/
      #   for more information and usage.
      # @option options [ Session ] :session The session to use.
      # @option options [ Boolean ] :deserialize_as_bson Whether to deserialize
      #   this message using BSON types instead of native Ruby types wherever
      #   possible.
      #
      # @return [ Array<Hash> ] Info for each collection in the database.
      #
      # @since 2.0.5
      def list_collections(options = {})
        session = client.get_session(options)
        collections_info(session, ServerSelector.primary, options)
      end

      # Create the new database view.
      #
      # @example Create the new database view.
      #   Database::View.new(database)
      #
      # @param [ Database ] database The database.
      # @param [ Hash ] options The options to configure the view with.
      #
      # @option options [ :cursor_lifetime | :iteration ] :timeout_mode How to interpret
      #   :timeout_ms (whether it applies to the lifetime of the cursor, or per
      #   iteration).
      # @option options [ Integer ] :timeout_ms The operation timeout in milliseconds.
      #    Must be a non-negative integer. An explicit value of 0 means infinite.
      #    The default value is unset which means the value is inherited from
      #    the database or the client.
      #
      # @since 2.0.0
      def initialize(database, options = {})
        @database = database
        @operation_timeout_ms = options.delete(:timeout_ms)

        validate_timeout_mode!(options)

        @batch_size =  nil
        @limit = nil
        @collection = @database[Database::COMMAND]
      end

      # @api private
      attr_reader :database

      # @return [ Integer | nil | The timeout_ms value that was passed as an
      #   option to the view.
      #
      # @api private
      attr_reader :operation_timeout_ms

      # Execute an aggregation on the database view.
      #
      # @example Aggregate documents.
      #   view.aggregate([
      #     { "$listLocalSessions" => {} }
      #   ])
      #
      # @param [ Array<Hash> ] pipeline The aggregation pipeline.
      # @param [ Hash ] options The aggregation options.
      #
      # @return [ Collection::View::Aggregation ] The aggregation object.
      #
      # @since 2.10.0
      # @api private
      def aggregate(pipeline, options = {})
        Collection::View::Aggregation.new(self, pipeline, options)
      end

      # The timeout_ms value to use for this operation; either specified as an
      # option to the view, or inherited from the database.
      #
      # @return [ Integer | nil ] the timeout_ms for this operation
      def timeout_ms
        operation_timeout_ms || database.timeout_ms
      end

      # @return [ Hash ] timeout_ms value set on the operation level (if any).
      #
      # @api private
      def operation_timeouts(opts = {})
        {}.tap do |result|
          if opts[:timeout_ms] || operation_timeout_ms
            result[:operation_timeout_ms] = opts.delete(:timeout_ms) || operation_timeout_ms
          else
            result[:inherited_timeout_ms] = database.timeout_ms
          end
        end
      end

      private

      def collections_info(session, server_selector, options = {}, &block)
        description = nil
        context = Operation::Context.new(
          client: client,
          session: session,
          operation_timeouts: operation_timeouts(options)
        )
        cursor = read_with_retry_cursor(session, server_selector, self, context: context) do |server|
          # TODO take description from the connection used to send the query
          # once https://jira.mongodb.org/browse/RUBY-1601 is fixed.
          description = server.description
          send_initial_query(server, session, context, options)
        end
        # On 3.0+ servers, we get just the collection names.
        # On 2.6 server, we get collection names prefixed with the database
        # name. We need to filter system collections out here because
        # in the caller we don't know which server version executed the
        # command and thus what the proper filtering logic should be
        # (it is valid for collection names to have dots, thus filtering out
        # collections named system.* here for 2.6 servers would actually
        # filter out collections in the system database).
        if description.server_version_gte?('3.0')
          cursor.reject do |doc|
            doc['name'].start_with?('system.') || doc['name'].include?('$')
          end
        else
          cursor.reject do |doc|
            doc['name'].start_with?("#{database.name}.system") || doc['name'].include?('$')
          end
        end
      end

      def collections_info_spec(session, options = {})
        { selector: {
            listCollections: 1,
            cursor: batch_size ? { batchSize: batch_size } : {} },
          db_name: @database.name,
          session: session
        }.tap do |spec|
          spec[:selector][:nameOnly] = true if options[:name_only]
          spec[:selector][:filter] = options[:filter] if options[:filter]
          spec[:selector][:authorizedCollections] = true if options[:authorized_collections]
          spec[:comment] = options[:comment] if options[:comment]
        end
      end

      def initial_query_op(session, options = {})
        Operation::CollectionsInfo.new(collections_info_spec(session, options))
      end

      # Sends command that obtains information about the database.
      #
      # This command returns a cursor, so there could be additional commands,
      # therefore this method is called send *initial* command.
      #
      # @param [ Server ] server Server to send the query to.
      # @param [ Session ] session Session that should be used to send the query.
      # @param [ Hash ] options
      # @option options [ Hash | nil ] :filter A query expression to filter
      #   the list of collections.
      # @option options [ true | false | nil ] :name_only A flag to indicate
      #   whether the command should return just the collection/view names
      #   and type or return both the name and other information.
      # @option options [ true | false | nil ] :authorized_collections A flag,
      #   when set to true and used with name_only: true, that allows a user
      #   without the required privilege (i.e. listCollections
      #   action on the database) to run the command when access control
      #   is enforced.
      # @option options [ Object | nil ] :comment A user-provided comment to attach
      #   to this command.
      # @option options [ true | false | nil ] :deserialize_as_bson Whether the
      #   query results should be deserialized to BSON types, or to Ruby
      #   types (where possible).
      #
      # @return [ Operation::Result ] Result of the query.
      def send_initial_query(server, session, context, options = {})
        opts = options.dup
        execution_opts = {}
        if opts.key?(:deserialize_as_bson)
          execution_opts[:deserialize_as_bson] = opts.delete(:deserialize_as_bson)
        end
        if server.load_balancer?
          connection = server.pool.check_out(context: context)
          initial_query_op(session, opts).execute_with_connection(
            connection,
            context: context,
            options: execution_opts
          )
        else
          initial_query_op(session, opts).execute(
            server,
            context: context,
            options: execution_opts
          )
        end
      end
    end
  end
end

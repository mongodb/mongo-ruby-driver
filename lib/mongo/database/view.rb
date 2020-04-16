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

module Mongo
  class Database

    # A class representing a view of a database.
    #
    # @since 2.0.0
    class View
      extend Forwardable
      include Enumerable
      include Retryable

      def_delegators :@database, :cluster, :read_preference, :client
      # @api private
      def_delegators :@database, :server_selector, :read_concern
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
      #
      # @return [ Array<String> ] The names of all non-system collections.
      #
      # @since 2.0.0
      def collection_names(options = {})
        @batch_size = options[:batch_size]
        session = client.send(:get_session, options)
        cursor = read_with_retry_cursor(session, ServerSelector.primary, self) do |server|
          send_initial_query(server, session, name_only: true)
        end
        cursor.map do |info|
          if cursor.server.with_connection { |connection| connection.features }.list_collections_enabled?
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
      #   See https://docs.mongodb.com/manual/reference/command/listCollections/
      #   for more information and usage.
      #
      # @return [ Array<Hash> ] Info for each collection in the database.
      #
      # @since 2.0.5
      def list_collections(**options)
        session = client.send(:get_session)
        collections_info(session, ServerSelector.primary, **options)
      end

      # Create the new database view.
      #
      # @example Create the new database view.
      #   View::Index.new(database)
      #
      # @param [ Database ] database The database.
      #
      # @since 2.0.0
      def initialize(database)
        @database = database
        @batch_size =  nil
        @limit = nil
        @collection = @database[Database::COMMAND]
      end

      # @api private
      attr_reader :database

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
      # @return [ Aggregation ] The aggregation object.
      #
      # @since 2.10.0
      # @api private
      def aggregate(pipeline, options = {})
        Collection::View::Aggregation.new(self, pipeline, options)
      end

      private

      def collections_info(session, server_selector, options = {}, &block)
        description = nil
        cursor = read_with_retry_cursor(session, server_selector, self) do |server|
          # TODO take description from the connection used to send the query
          # once https://jira.mongodb.org/browse/RUBY-1601 is fixed.
          description = server.description
          send_initial_query(server, session, options)
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
          docs = cursor.reject do |doc|
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
        end
      end

      def initial_query_op(session, options = {})
        Operation::CollectionsInfo.new(collections_info_spec(session, options))
      end

      def send_initial_query(server, session, options = {})
        initial_query_op(session, options).execute(server, client: client)
      end
    end
  end
end

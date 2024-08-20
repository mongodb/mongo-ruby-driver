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
  module Index

    # A class representing a view of indexes.
    #
    # @since 2.0.0
    class View
      extend Forwardable
      include Enumerable
      include Retryable
      include Mongo::CursorHost
      include Cursor::NonTailable

      # @return [ Collection ] collection The indexes collection.
      attr_reader :collection

      # @return [ Integer ] batch_size The size of the batch of results
      #   when sending the listIndexes command.
      attr_reader :batch_size

      # @return [ Integer | nil | The timeout_ms value that was passed as an
      #   option to the view.
      #
      # @api private
      attr_reader :operation_timeout_ms

      def_delegators :@collection, :cluster, :database, :read_preference, :write_concern, :client
      def_delegators :cluster, :next_primary

      # The index key field.
      #
      # @since 2.0.0
      KEY = 'key'.freeze

      # The index name field.
      #
      # @since 2.0.0
      NAME = 'name'.freeze

      # The mappings of Ruby index options to server options.
      #
      # @since 2.0.0
      OPTIONS = {
        :background => :background,
        :bits => :bits,
        :bucket_size => :bucketSize,
        :default_language => :default_language,
        :expire_after => :expireAfterSeconds,
        :expire_after_seconds => :expireAfterSeconds,
        :key => :key,
        :language_override => :language_override,
        :max => :max,
        :min => :min,
        :name => :name,
        :partial_filter_expression => :partialFilterExpression,
        :sparse => :sparse,
        :sphere_version => :'2dsphereIndexVersion',
        :storage_engine => :storageEngine,
        :text_version => :textIndexVersion,
        :unique => :unique,
        :version => :v,
        :weights => :weights,
        :collation => :collation,
        :comment => :comment,
        :wildcard_projection => :wildcardProjection,
      }.freeze

      # Drop an index by its name.
      #
      # @example Drop an index by its name.
      #   view.drop_one('name_1')
      #
      # @param [ String ] name The name of the index.
      # @param [ Hash ] options Options for this operation.
      #
      # @option options [ Object ] :comment A user-provided
      #   comment to attach to this command.
      #
      # @return [ Result ] The response.
      #
      # @since 2.0.0
      def drop_one(name, options = {})
        raise Error::MultiIndexDrop.new if name == Index::ALL
        drop_by_name(name, options)
      end

      # Drop all indexes on the collection.
      #
      # @example Drop all indexes on the collection.
      #   view.drop_all
      #
      # @param [ Hash ] options Options for this operation.
      #
      # @option options [ Object ] :comment A user-provided
      #   comment to attach to this command.
      #
      # @return [ Result ] The response.
      #
      # @since 2.0.0
      def drop_all(options = {})
        drop_by_name(Index::ALL, options)
      end

      # Creates an index on the collection.
      #
      # @example Create a unique index on the collection.
      #   view.create_one({ name: 1 }, { unique: true })
      #
      # @param [ Hash ] keys A hash of field name/direction pairs.
      # @param [ Hash ] options Options for this index.
      #
      # @option options [ true, false ] :unique (false) If true, this index will enforce
      #   a uniqueness constraint on that field.
      # @option options [ true, false ] :background (false) If true, the index will be built
      #   in the background (only available for server versions >= 1.3.2 )
      # @option options [ true, false ] :drop_dups (false) If creating a unique index on
      #   this collection, this option will keep the first document the database indexes
      #   and drop all subsequent documents with duplicate values on this field.
      # @option options [ Integer ] :bucket_size (nil) For use with geoHaystack indexes.
      #   Number of documents to group together within a certain proximity to a given
      #   longitude and latitude.
      # @option options [ Integer ] :max (nil) Specify the max latitude and longitude for
      #   a geo index.
      # @option options [ Integer ] :min (nil) Specify the min latitude and longitude for
      #   a geo index.
      # @option options [ Hash ] :partial_filter_expression  Specify a filter for a partial
      #   index.
      # @option options [ Boolean ] :hidden When :hidden is true, this index will
      #   exist on the collection but not be used by the query planner when
      #   executing operations.
      # @option options [ String | Integer ] :commit_quorum Specify how many
      #   data-bearing members of a replica set, including the primary, must
      #   complete the index builds successfully before the primary marks
      #   the indexes as ready. Potential values are:
      #   - an integer from 0 to the number of members of the replica set
      #   - "majority" indicating that a majority of data bearing nodes must vote
      #   - "votingMembers" which means that all voting data bearing nodes must vote
      # @option options [ Session ] :session The session to use for the operation.
      # @option options [ Object ] :comment A user-provided
      #   comment to attach to this command.
      #
      # @note Note that the options listed may be subset of those available.
      # See the MongoDB documentation for a full list of supported options by server version.
      #
      # @return [ Result ] The response.
      #
      # @since 2.0.0
      def create_one(keys, options = {})
        options = options.dup

        create_options = {}
        if session = @options[:session]
          create_options[:session] = session
        end
        %i(commit_quorum session comment timeout_ms max_time_ms).each do |key|
          if value = options.delete(key)
            create_options[key] = value
          end
        end
        create_many({ key: keys }.merge(options), create_options)
      end

      # Creates multiple indexes on the collection.
      #
      # @example Create multiple indexes.
      #   view.create_many([
      #     { key: { name: 1 }, unique: true },
      #     { key: { age: -1 }, background: true }
      #   ])
      #
      # @example Create multiple indexes with options.
      #   view.create_many(
      #     { key: { name: 1 }, unique: true },
      #     { key: { age: -1 }, background: true },
      #     { commit_quorum: 'majority' }
      #   )
      #
      # @note On MongoDB 3.0.0 and higher, the indexes will be created in
      #   parallel on the server.
      #
      # @param [ Array<Hash> ] models The index specifications. Each model MUST
      #   include a :key option, except for the last item in the Array, which
      #   may be a Hash specifying options relevant to the createIndexes operation.
      #   The following options are accepted:
      #   - commit_quorum: Specify how many data-bearing members of a replica set,
      #     including the primary, must complete the index builds successfully
      #     before the primary marks the indexes as ready. Potential values are:
      #     - an integer from 0 to the number of members of the replica set
      #     - "majority" indicating that a majority of data bearing nodes must vote
      #     - "votingMembers" which means that all voting data bearing nodes must vote
      #   - session: The session to use.
      #   - comment: A user-provided comment to attach to this command.
      #
      # @return [ Result ] The result of the command.
      #
      # @since 2.0.0
      def create_many(*models)
        models = models.flatten
        options = {}
        if models && !models.last.key?(:key)
          options = models.pop
        end

        client.with_session(@options.merge(options)) do |session|
          server = next_primary(nil, session)

          indexes = normalize_models(models, server)
          indexes.each do |index|
            if index[:bucketSize] || index['bucketSize']
              client.log_warn("Haystack indexes (bucketSize index option) are deprecated as of MongoDB 4.4")
            end
          end

          spec = {
            indexes: indexes,
            db_name: database.name,
            coll_name: collection.name,
            session: session,
            commit_quorum: options[:commit_quorum],
            write_concern: write_concern,
            comment: options[:comment],
          }
          context = Operation::Context.new(
            client: client,
            session: session,
            operation_timeouts: operation_timeouts(options)
          )
          Operation::CreateIndex.new(spec).execute(server, context: context)
        end
      end

      # Convenience method for getting index information by a specific name or
      # spec.
      #
      # @example Get index information by name.
      #   view.get('name_1')
      #
      # @example Get index information by the keys.
      #   view.get(name: 1)
      #
      # @param [ Hash, String ] keys_or_name The index name or spec.
      #
      # @return [ Hash ] The index information.
      #
      # @since 2.0.0
      def get(keys_or_name)
        find do |index|
          (index[NAME] == keys_or_name) || (index[KEY] == normalize_keys(keys_or_name))
        end
      end

      # Iterate over all indexes for the collection.
      #
      # @example Get all the indexes.
      #   view.each do |index|
      #     ...
      #   end
      #
      # @since 2.0.0
      def each(&block)
        session = client.get_session(@options)
        context = Operation::Context.new(
          client: client,
          session: session,
          operation_timeouts: operation_timeouts(@options)
        )

        cursor = read_with_retry_cursor(session, ServerSelector.primary, self, context: context) do |server|
          send_initial_query(server, session, context)
        end
        if block_given?
          cursor.each do |doc|
            yield doc
          end
        else
          cursor.to_enum
        end
      end

      # Create the new index view.
      #
      # @example Create the new index view.
      #   View::Index.new(collection)
      #
      # @param [ Collection ] collection The collection.
      # @param [ Hash ] options Options for getting a list of indexes.
      #
      # @option options [ Integer ] :batch_size The batch size for results
      #   returned from the listIndexes command.
      # @option options [ :cursor_lifetime | :iteration ] :timeout_mode How to interpret
      #   :timeout_ms (whether it applies to the lifetime of the cursor, or per
      #   iteration).
      # @option options [ Integer ] :timeout_ms The operation timeout in milliseconds.
      #    Must be a non-negative integer. An explicit value of 0 means infinite.
      #    The default value is unset which means the value is inherited from
      #    the collection or the database or the client.
      #
      # @since 2.0.0
      def initialize(collection, options = {})
        @collection = collection
        @operation_timeout_ms = options.delete(:timeout_ms)

        validate_timeout_mode!(options)

        @batch_size = options[:batch_size]
        @options = options
      end

      # The timeout_ms value to use for this operation; either specified as an
      # option to the view, or inherited from the collection.
      #
      # @return [ Integer | nil ] the timeout_ms for this operation
      def timeout_ms
        operation_timeout_ms || collection.timeout_ms
      end

      # @return [ Hash ] timeout_ms value set on the operation level (if any),
      #   and/or timeout_ms that is set on collection/database/client level (if any).
      #
      # @api private
      def operation_timeouts(opts = {})
        {}.tap do |result|
          if opts[:timeout_ms] || operation_timeout_ms
            result[:operation_timeout_ms] = opts.delete(:timeout_ms) || operation_timeout_ms
          else
            result[:inherited_timeout_ms] = collection.timeout_ms
          end
        end
      end

      private

      def drop_by_name(name, opts = {})
        client.send(:with_session, @options) do |session|
          spec = {
            db_name: database.name,
            coll_name: collection.name,
            index_name: name,
            session: session,
            write_concern: write_concern,
          }
          spec[:comment] = opts[:comment] unless opts[:comment].nil?
          server = next_primary(nil, session)
          context = Operation::Context.new(
            client: client,
            session: session,
            operation_timeouts: operation_timeouts(opts)
          )
          Operation::DropIndex.new(spec).execute(server, context: context)
        end
      end

      def index_name(spec)
        spec.to_a.join('_')
      end

      def indexes_spec(session)
        { selector: {
            listIndexes: collection.name,
            cursor: batch_size ? { batchSize: batch_size } : {} },
          coll_name: collection.name,
          db_name: database.name,
          session: session
        }
      end

      def initial_query_op(session)
        Operation::Indexes.new(indexes_spec(session))
      end

      def limit; -1; end

      def normalize_keys(spec)
        return false if spec.is_a?(String)
        Options::Mapper.transform_keys_to_strings(spec)
      end

      def normalize_models(models, server)
        models.map do |model|
          # Transform options first which gives us a mutable hash
          Options::Mapper.transform(model, OPTIONS).tap do |model|
            model[:name] ||= index_name(model.fetch(:key))
          end
        end
      end

      def send_initial_query(server, session, context)
        if server.load_balancer?
          connection = server.pool.check_out(context: context)
          initial_query_op(session).execute_with_connection(connection, context: context)
        else
          initial_query_op(session).execute(server, context: context)
        end
      end
    end
  end
end

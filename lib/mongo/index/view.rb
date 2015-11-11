# Copyright (C) 2014-2015 MongoDB, Inc.
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
  module Index

    # A class representing a view of indexes.
    #
    # @since 2.0.0
    class View
      extend Forwardable
      include Enumerable

      # @return [ Collection ] collection The indexes collection.
      attr_reader :collection

      # @return [ Integer ] batch_size The size of the batch of results
      #   when sending the listIndexes command.
      attr_reader :batch_size

      def_delegators :@collection, :cluster, :database, :read_preference
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
        :weights => :weights
      }.freeze

      # Drop an index by its name.
      #
      # @example Drop an index by its name.
      #   view.drop_one('name_1')
      #
      # @param [ String ] name The name of the index.
      #
      # @return [ Result ] The response.
      #
      # @since 2.0.0
      def drop_one(name)
        raise Error::MultiIndexDrop.new if name == Index::ALL
        drop_by_name(name)
      end

      # Drop all indexes on the collection.
      #
      # @example Drop all indexes on the collection.
      #   view.drop_all
      #
      # @return [ Result ] The response.
      #
      # @since 2.0.0
      def drop_all
        drop_by_name(Index::ALL)
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
      #
      # @note Note that the options listed may be subset of those available.
      # See the MongoDB documentation for a full list of supported options by server version.
      #
      # @return [ Result ] The response.
      #
      # @since 2.0.0
      def create_one(keys, options = {})
        create_many({ key: keys }.merge(options))
      end

      # Creates multiple indexes on the collection.
      #
      # @example Create multiple indexes.
      #   view.create_many([
      #     { key: { name: 1 }, unique: true },
      #     { key: { age: -1 }, background: true }
      #   ])
      #
      # @note On MongoDB 3.0.0 and higher, the indexes will be created in
      #   parallel on the server.
      #
      # @param [ Array<Hash> ] models The index specifications. Each model MUST
      #   include a :key option.
      #
      # @return [ Result ] The result of the command.
      #
      # @since 2.0.0
      def create_many(*models)
        Operation::Write::CreateIndex.new(
          indexes: normalize_models(models.flatten),
          db_name: database.name,
          coll_name: collection.name,
        ).execute(next_primary.context)
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
        server = next_primary(false)
        cursor = Cursor.new(self, send_initial_query(server), server).to_enum
        cursor.each do |doc|
          yield doc
        end if block_given?
        cursor
      end

      # Create the new index view.
      #
      # @example Create the new index view.
      #   View::Index.new(collection)
      #
      # @param [ Collection ] collection The collection.
      # @param [ Hash ] options Options for getting a list of indexes.
      #   Only relevant for when the listIndexes command is used with server
      #   versions >=2.8.
      #
      # @option options [ Integer ] :batch_size The batch size for results
      #   returned from the listIndexes command.
      #
      # @since 2.0.0
      def initialize(collection, options = {})
        @collection = collection
        @batch_size = options[:batch_size]
      end

      private

      def drop_by_name(name)
        Operation::Write::DropIndex.new(
          db_name: database.name,
          coll_name: collection.name,
          index_name: name
        ).execute(next_primary.context)
      end

      def index_name(spec)
        spec.to_a.join('_')
      end

      def indexes_spec
        { selector: {
            listIndexes: collection.name,
            cursor: batch_size ? { batchSize: batch_size } : {} },
          coll_name: collection.name,
          db_name: database.name }
      end

      def initial_query_op
        Operation::Commands::Indexes.new(indexes_spec)
      end

      def limit; -1; end

      def normalize_keys(spec)
        return false if spec.is_a?(String)
        Options::Mapper.transform_keys_to_strings(spec)
      end

      def normalize_models(models)
        with_generated_names(models).map do |model|
          Options::Mapper.transform(model, OPTIONS)
        end
      end

      def send_initial_query(server)
        initial_query_op.execute(server.context)
      end

      def with_generated_names(models)
        models.dup.each do |model|
          unless model[:name]
            model[:name] = index_name(model[:key])
          end
        end
      end
    end
  end
end

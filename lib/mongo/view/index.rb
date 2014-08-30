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
  module View

    # A class representing a MongoDB Index.
    #
    # @since 2.0.0
    class Index
      extend Forwardable
      include Enumerable

      # @return [ Collection ] collection The indexes collection.
      attr_reader :collection

      def_delegators :@collection, :cluster, :database, :server_preference
      def_delegators :cluster, :next_primary

      # Specify ascending order for an index.
      #
      # @since 2.0.0
      ASCENDING = 1

      # Specify descending order for an index.
      #
      # @since 2.0.0
      DESCENDING = -1

      # Specify a 2d Geo index.
      #
      # @since 2.0.0
      GEO2D = '2d'.freeze

      # Specify a 2d sphere Geo index.
      #
      # @since 2.0.0
      GEO2DSPHERE = '2dsphere'.freeze

      # Specify a geoHaystack index.
      #
      # @since 2.0.0
      GEOHAYSTACK = 'geoHaystack'.freeze

      # Encodes a text index.
      #
      # @since 2.0.0
      TEXT = 'text'.freeze

      # Specify a hashed index.
      #
      # @since 2.0.0
      HASHED = 'hashed'.freeze

      # Constant for the indexes collection.
      #
      # @since 2.0.0
      COLLECTION = 'system.indexes'.freeze

      # The index key field.
      #
      # @since 2.0.0
      KEY = 'key'.freeze

      # The index name field.
      #
      # @since 2.0.0
      NAME = 'name'.freeze

      # Drop an index by its specification.
      #
      # @example Drop the index by spec.
      #   view.drop(name: 1)
      #
      # @example Drop an index by its name.
      #   view.drop('name_1')
      #
      # @param [ Hash, String ] spec The index spec or name to drop.
      #
      # @return [ Operation::Write::DropIndex::Response ] The response.
      #
      # @since 2.0.0
      def drop(spec)
        Operation::Write::DropIndex.new(
          db_name: database.name,
          coll_name: collection.name,
          index_name: spec.is_a?(String) ? spec : index_name(spec)
        ).execute(next_primary.context)
      end

      # Drop all indexes on the collection.
      #
      # @example Drop all indexes on the collection.
      #   view.drop_all
      #
      # @return [ Operation::Write::DropIndex::Response ] The response.
      #
      # @since 2.0.0
      def drop_all
        drop('*')
      end

      # Calls create_index and sets a flag not to do so again for another X minutes.
      #  This time can be specified as an option when initializing a Mongo::DB object
      #  as options. Any changes to an index will be propagated through regardless of
      #  cache time (e.g., a change of index direction).
      #
      # @param [ Hash ] spec A hash of field name/direction pairs.
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
      #
      # @return [ EnsureIndex::Response ] The response.
      #
      # @since 2.0.0
      def ensure(spec, options = {})
        Operation::Write::EnsureIndex.new(
          index: spec,
          db_name: database.name,
          coll_name: collection.name,
          index_name: options[:name] || index_name(spec),
          options: options
        ).execute(next_primary.context)
      end

      # Convenience method for getting index information by a specific name or
      # spec.
      #
      # @example Get index information by name.
      #   view.get('name_1')
      #
      # @example Get index information by spec.
      #   view.get(name: 1)
      #
      # @param [ Hash, String ] spec The index name or spec.
      #
      # @return [ Hash ] The index information.
      #
      # @since 2.0.0
      def get(spec)
        find do |index|
          (index[NAME] == spec) || (index[KEY] == normalize_keys(spec))
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
        server = server_preference.select_servers(cluster.servers).first
        Operation::Read::Indexes.new(
          db_name: database.name,
          coll_name: collection.name
        ).execute(server.context).documents.each(&block)
      end

      # Create the new index view.
      #
      # @example Create the new index view.
      #   View::Index.new(collection)
      #
      # @param [ Collection ] collection The collection.
      #
      # @since 2.0.0
      def initialize(collection)
        @collection = collection
      end

      private

      def index_name(spec)
        spec.to_a.join('_')
      end

      def normalize_keys(spec)
        return false if spec.is_a?(String)
        spec.reduce({}) do |normalized, (key, value)|
          normalized[key.to_s] = value
          normalized
        end
      end
    end
  end
end

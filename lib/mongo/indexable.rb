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

  # A class representing a MongoDB Index.
  #
  # @since 2.0.0
  module Indexable

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
    SYSTEM_INDEXES = 'system.indexes'.freeze

    INDEX_KEY = 'key'.freeze
    INDEX_NAME = 'name'.freeze

    # Drop an index by its specification.
    #
    # @example Drop the index by spec.
    #   indexable.drop_index(name: 1)
    #
    # @example Drop an index by its name.
    #   indexable.drop_index('name_1')
    #
    # @param [ Hash, String ] spec The index spec or name to drop.
    #
    # @return [ Operation::Write::DropIndex::Response ] The response.
    #
    # @since 2.0.0
    def drop_index(spec)
      server = server_preference.primary(cluster.servers).first
      Operation::Write::DropIndex.new(
        db_name: database.name,
        coll_name: name,
        index_name: spec.is_a?(String) ? spec : index_name(spec)
      ).execute(server.context)
    end

    # Drop all indexes on the collection.
    #
    # @example Drop all indexes on the collection.
    #   indexable.drop_indexes
    #
    # @return [ Operation::Write::DropIndex::Response ] The response.
    #
    # @since 2.0.0
    def drop_indexes
      drop_index('*')
    end

    # Calls create_index and sets a flag not to do so again for another X minutes.
    #  This time can be specified as an option when initializing a Mongo::DB object
    #  as options. Any changes to an index will be propagated through regardless of
    #  cache time (e.g., a change of index direction).
    #
    # @param [ Hash ] spec A hash of field name/direction pairs.
    # @param [ Hash ] opts Options for this index.
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
    def ensure_index(spec, options = {})
      server = server_preference.primary(cluster.servers).first
      Operation::Write::EnsureIndex.new(
        index: spec,
        db_name: database.name,
        coll_name: name,
        index_name: options[:name] || index_name(spec),
        opts: options
      ).execute(server.context)
    end

    # Convenience method for getting index information by a specific name or
    # spec.
    #
    # @example Get index information by name.
    #   indexable.find_index('name_1')
    #
    # @example Get index information by spec.
    #   indexable.find_index(name: 1)
    #
    # @param [ Hash, String ] spec The index name or spec.
    #
    # @return [ Hash ] The index information.
    #
    # @since 2.0.0
    def find_index(spec)
      indexes.documents.find do |index|
        (index[INDEX_NAME] == spec) || (index[INDEX_KEY] == normalize_keys(spec))
      end
    end

    # Get all the indexes for the collection.
    #
    # @example Get all the indexes.
    #   indexable.indexes
    #
    # @return [ Array<Hash> ] All the collection's indexes.
    #
    # @since 2.0.0
    def indexes
      server = server_preference.select_servers(cluster.servers).first
      Operation::Read::Indexes.new(
        db_name: database.name,
        coll_name: name
      ).execute(server.context)
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

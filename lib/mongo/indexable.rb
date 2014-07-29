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

  # Specify ascending order for an index.
  #
  # @since 2.0.0
  ASCENDING = 1

  # Specify descending order for an index.
  #
  # @since 2.0.0
  DESCENDING = -1

  # The system name for the collection containing information about indexes.
  #
  # @since 2.0.0
  SYSTEM_INDEXES = 'system.indexes'.freeze

  # A class representing a MongoDB Index.
  #
  # @since 2.0.0
  module Indexable

    # Specify a 2d Geo index.
    #
    # @since 2.0.0
    GEO2D = '2d'

    # Specify a 2d sphere Geo index.
    #
    # @since 2.0.0
    GEO2DSPHERE = '2dsphere'

    # Specify a geoHaystack index.
    #
    # @since 2.0.0
    GEOHAYSTACK = 'geoHaystack'

    # Encodes a text index.
    #
    # @since 2.0.0
    TEXT = 'text'

    # Specify a hashed index.
    #
    # @since 2.0.0
    HASHED = 'hashed'

    # An array of allowable index values.
    #
    # @since 2.0.0
    INDEX_TYPES = {
      'ASCENDING'   => ASCENDING,
      'DESCENDING'  => DESCENDING,
      'GEO2D'       => GEO2D,
      'GEO2DSPHERE' => GEO2DSPHERE,
      'GEOHAYSTACK' => GEOHAYSTACK,
      'TEXT'        => TEXT,
      'HASHED'      => HASHED
    }

    # Time indexes are kept in client cache until they are considered expired.
    #
    # @since 2.0.0
    TIME_TO_EXPIRE = 300.freeze #5 minutes.

    # Create a new index on this collection.
    #
    # @param [ String, Array ] spec A single field name or an array of
    #   [field_name, type] pairs.
    # @param [ Hash ] opts Options for this index.
    #
    # @option opts [ true, false ] :unique (false) If true, this index will enforce
    #   a uniqueness constraint on that field.
    # @option opts [ true, false ] :background (false) If true, the index will be built
    #   in the background (only available for server versions >= 1.3.2 )
    # @option opts [ true, false ] :drop_dups (false) If creating a unique index on
    #   this collection, this option will keep the first document the database indexes
    #   and drop all subsequent documents with duplicate values on this field.
    # @option opts [ Integer ] :bucket_size (nil) For use with geoHaystack indexes.
    #   Number of documents to group together within a certain proximity to a given
    #   longitude and latitude.
    # @option opts [ Integer ] :max (nil) Specify the max latitude and longitude for
    #   a geo index.
    # @option opts [ Integer ] :min (nil) Specify the min latitude and longitude for
    #   a geo index.
    #
    # @note if your code calls create_index frequently, you can use
    #  Collection#ensure_index instead to avoid redundant index creation.
    #
    # @example Creating a compound index using a hash: (Ruby 1.9+ Syntax)
    #   @posts.create_index({'subject' => Mongo::ASCENDING,
    #                        'created_at' => Mongo::DESCENDING})
    #
    # @example Creating a compound index:
    #   @posts.create_index([['subject', Mongo::ASCENDING],
    #                        ['created_at', Mongo::DESCENDING]])
    #
    # @example Creating a geospatial index using a hash: (Ruby 1.9+ Syntax)
    #   @restaurants.create_index(:location => Mongo::GEO2D)
    #
    # @example Creating a geospatial index:
    #   @restaurants.create_index([['location' => Mongo::GEO2D]]))
    #
    #   # Note that this will work only if 'location' represents x,y coordinates:
    #   {'location': [0, 50]}
    #   {'location': {'x' => 0, 'y' => 50}}
    #   {'location': {'latitude' => 0, 'longitude' => 50}}
    #
    # @example A geospatial index with alternate longitude and latitude:
    #   @restaurants.create_index([['location', Mongo::GEO2D]],
    #                             :min => 500, :max => 500)
    #
    # @return [ String ] the name of the index created.
    #
    # @since 2.0.0
    def create_index(spec, opts={})
      apply_index(parse_index_spec(spec), opts)
    end

    # Drop a specified index by name.
    #
    # @param [ String ] name The index to drop.
    #
    # @since 2.0.0
    def drop_index(name)
      drop_index_by_name(name)
    end

    # Drop all indexes on this collection.
    #
    # @since 2.0.0
    def drop_indexes
      drop_index_by_name('*')
    end

    # Calls create_index and sets a flag not to do so again for another X minutes.
    #  This time can be specified as an option when initializing a Mongo::DB object
    #  as options. Any changes to an index will be propagated through regardless of
    #  cache time (e.g., a change of index direction).
    #
    # @param [ String, Array ] spec A single field name or an array of
    #   [field_name, type] pairs.
    # @param [ Hash ] opts Options for this index.
    #
    # @option opts [ true, false ] :unique (false) If true, this index will enforce
    #   a uniqueness constraint on that field.
    # @option opts [ true, false ] :background (false) If true, the index will be built
    #   in the background (only available for server versions >= 1.3.2 )
    # @option opts [ true, false ] :drop_dups (false) If creating a unique index on
    #   this collection, this option will keep the first document the database indexes
    #   and drop all subsequent documents with duplicate values on this field.
    # @option opts [ Integer ] :bucket_size (nil) For use with geoHaystack indexes.
    #   Number of documents to group together within a certain proximity to a given
    #   longitude and latitude.
    # @option opts [ Integer ] :max (nil) Specify the max latitude and longitude for
    #   a geo index.
    # @option opts [ Integer ] :min (nil) Specify the min latitude and longitude for
    #   a geo index.
    #
    # @return [ String ] the name of the index.
    #
    # @since 2.0.0
    def ensure_index(spec, opts={})
      spec = parse_index_spec(spec)
      index = index_name(spec)

      apply_index(spec, opts) if expired?(index)
      client.index_cache({ index => Time.now.utc.to_i + TIME_TO_EXPIRE }, ns)
    end

    # Returns information about the indexes on this collection by name.
    #
    # @return [ Hash ] information about the collection's indexes, with index names
    #  as keys.
    #
    # @since 2.0.0
    def index_information
      info = {}
      system_indexes.find({ :ns => ns }).each do |index|
        info[index['name']] = index
      end
      info
    end

    private

    # Apply this index to this collection.
    #
    # @param [ Hash ] spec The index spec.
    # @param [ Hash ] opts Options for this index.
    #
    # @since 2.0.0
    def apply_index(spec, opts={})
      index = index_name(spec)
      selector = { :name => index, :key => spec }
      selector.merge!(opts)
      begin
        database.command({ :createIndexes => name, :indexes => [selector] })
      rescue OperationError => ex
        if Mongo::ErrorCode::COMMAND_NOT_FOUND.include?(ex.error_code)
          # @todo legacy index creation?
        else
          raise OperationError, "Failed to create index #{selector.inspect}" +
            "with the following error: #{ex.message}"
        end
      end
    end

    # Return true if this index has expired, or was never created (on this client).
    #   For use with ensure_index.
    #
    # @param [ String ] index The index name.
    #
    # @return [ true, false ] whether the index has gone stale.
    #
    # @since 2.0.0
    def expired?(index)
      time = client.index_cache(index, ns)
      return true unless time
      time < Time.now.utc.to_i
    end

    # Return a new Collection representing system.indexes on this collection's
    # database.
    #
    # @retun [ Collection ]
    #
    # @since 2.0.0
    def system_indexes
      Collection.new(database, SYSTEM_INDEXES)
    end

    # Drop an index from the given collection by name.
    #
    # @param [ String ] index The name of the index to drop.
    #
    # @since 2.0.0
    def drop_index_by_name(index)
      database.command({:deleteIndexes => name, :index => index})
    end

    # Parse the index spec into its proper form.
    #
    # @param [ String, Hash, Array ] Original index spec.
    #
    # @return [ Hash ] Parsed index spec.
    #
    # @since 2.0.0
    def parse_index_spec(spec)
      field_spec = {}
      if spec.is_a?(String) || spec.is_a?(Symbol)
        field_spec[spec.to_s] = 1
      elsif spec.is_a?(Hash)
        validate_index_types(spec.values)
        field_spec = spec
      elsif spec.is_a?(Array) && spec.all? {|field| field.is_a?(Arry)}
        spec.each do |field|
          validate_index_types(field[1])
          field_spec[field[0].to_s] = field[1]
        end
      else
        raise MongoArgumentError, "Invalid index specification #{spec.inspect};" +
          "should be either a hash, string, symbol, or an array of arrays."
      end
      field_spec
    end

    # Validate field types before index creation.
    #
    # @param [ Array ] *types the fields to validate.
    #
    # @since 2.0.0
    def validate_index_types(*types)
      types.flatten!
      types.each do |t|
        unless INDEX_TYPES.values.include?(t)
          raise MongoArgumentError, "Invalid index field #{t.inspect};" +
            "must be one of " + INDEX_TYPES.map {|k, v| "#{k} (#{v})"}.join(', ')
        end
      end
    end

    # Generate this index's name, based on its spec.
    #
    # @param [ Hash ] spec The index spec.
    #
    # @return [ String ] The name for this index.
    #
    # @since 2.0.0
    def index_name(spec)
      predicates = spec.collect do | field, type|
        "#{field}_#{type}"
      end
      predicates.join("_")
    end
  end
end

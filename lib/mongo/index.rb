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

  # A class representing a MongoDB Index.
  #
  # @since 2.0.0
  class Index

    # @return [ Mongo::Collection ] The collection this index belongs to.
    attr_reader :collection
    # @return [ Hash ] The field spec for this index.
    attr_reader :spec
    # @return [ Hash ] Options on this index.
    attr_reader :opts

    # Instantiate a new Index object.
    #
    # @param [ Hash ] spec The field spec for this index.
    # @param [ Mongo::Collection ] collection The collection this index is for.
    # @param [ Hash ] opts Options for this index.
    #
    # @option opts [ true, false ] :unique (false) If true, the index will enforce a
    #  uniqueness constraint.
    # @option opts [ true, false ] :drop_dups (nil) For a unique index added to a
    #  collection with existing keys, this option will keep the first document the
    #  database indexes, and subsequent documents with the same value for that key
    #  will be dropped from the collection.
    # @option opts [ true, false ] :background (false) Indicate that the index should
    #  be built in the background.  This feature is only available in MongoDB >= 1.3.2
    # @option opts [ Integer ] :bucket_size (nil) For use with geoHaystack indexes.
    #  Number of documents to group together within a certain proximity to a given
    #  latitude and longitude.
    # @option opts [ Integer ] :min (nil) The minimum longitude and latitude for a geo
    #  index.
    # @option opts [ Integer ] :max (nil) The maximum longitude and latitude for a geo
    #  index.
    #
    # @since 2.0.0
    def initialize(spec, collection, opts={})
      @spec = parse_index_spec(spec)
      @opts = opts
      @collection = collection
    end

    # Check if one index is equal to another object.  Will check the collection,
    # field spec, and options for equality.
    #
    # @param [ Object ] other The object to check.
    #
    # @return [ true, false ] Are these objects equal?
    #
    # @since 2.0.0
    def ==(other)
      return false unless other.is_a?(Index)
      return false unless spec == other.spec &&
        collection == other.collection
      puts "opts size: #{opts.size} and other: #{other.opts.size}"
      return false unless opts.size == other.opts.size
      @opts.each do |k, v|
        return false unless other.opts[k] == v
      end
      true
    end

    # Apply this index to its collection. This is equivalent to calling createIndex.
    #
    # @since 2.0.0
    def apply
      selector = { :name => name, :key => spec }
      selector.merge!(opts)
      begin
        collection.database.command({ :createIndexes => collection.name,
                                      :indexes => selector })
      rescue OperationError => ex
        if Mongo::ErrorCode::COMMAND_NOT_FOUND.include?(ex.error_code)
          # @todo legacy index creation?
        else
          raise OperationError, "Failed to create index #{selector.inspect}" +
            "with the following error: #{ex.message}"
        end
      end
      @applied = true
    end

    # Drop this index from its collection.
    #
    # @since 2.0.0
    def drop
      raise OperationError, "Cannot drop an unapplied index" unless @applied
      collection.database.command({:deleteIndexes => collection.name, :index => name})
    end

    # Drop an index from the given collection by name.
    #
    # @param [ Mongo::Collection ] target The indexed collection.
    # @param [ String ] The name of the index to drop.
    #
    # @since 2.0.0
    def self.drop(target, name)
      target.database.command({:deleteIndexes => target.name, :index => name})
    end

    # Drop all indexes on this collection.
    #
    # @param [ Mongo::Collection ] The indexed collection.
    #
    # @since 2.0.0
    def self.drop_all(collection)
      collection.database.command({:deleteIndexes => collection.name, :index => '*'})
    end

    # Return this index's name.
    #
    # @return [ String ] A name for this index.
    #
    # @since 2.0.0
    def name
      @name ||= generate_name
    end

    private

    # Parse the index spec into its proper form.
    #
    # @param [ Hash ] Original index spec.
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
    # @return [ String ] The name for this index.
    #
    # @since 2.0.0
    def generate_name
      predicates = spec.collect do | field, type|
        "#{field}_#{type}"
      end
      predicates.join("_")
    end
  end
end

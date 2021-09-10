# frozen_string_literal: true
# encoding: utf-8

# Copyright (C) 2015-2020 MongoDB Inc.
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

module Mongo

  # Represents a DBRef document in the database.
  #
  # @since 2.1.0
  class DBRef
    include BSON::JSON

    # The constant for the collection reference field.
    #
    # @since 2.1.0
    COLLECTION = '$ref'.freeze

    # The constant for the id field.
    #
    # @since 2.1.0
    ID = '$id'.freeze

    # The constant for the database field.
    #
    # @since 2.1.0
    DATABASE = '$db'.freeze

    # @return [ String ] collection The collection name.
    attr_reader :collection

    # @return [ BSON::ObjectId ] id The referenced document id.
    attr_reader :id

    # @return [ String ] database The database name.
    attr_reader :database

    # Get the DBRef as a JSON document
    #
    # @example Get the DBRef as a JSON hash.
    #   dbref.as_json
    #
    # @return [ Hash ] The max key as a JSON hash.
    #
    # @since 2.1.0
    def as_json(*args)
      document = { COLLECTION => collection, ID => id }
      document.merge!(DATABASE => database) if database
      document
    end

    # Instantiate a new DBRef.
    #
    # @example Create the DBRef.
    #   Mongo::DBRef.new('users', id, 'database')
    #
    # @param [ String ] collection The collection name.
    # @param [ BSON::ObjectId ] id The object id.
    # @param [ String ] database The database name.
    #
    # @since 2.1.0
    def initialize(collection, id, database = nil)
      @collection = collection
      @id = id
      @database = database
    end

    # Converts the DBRef to raw BSON.
    #
    # @example Convert the DBRef to raw BSON.
    #   dbref.to_bson
    #
    # @param [ BSON::ByteBuffer ] buffer The encoded BSON buffer to append to.
    # @param [ true, false ] validating_keys Whether keys should be validated when serializing.
    #
    # @return [ String ] The raw BSON.
    #
    # @since 2.1.0
    def to_bson(buffer = BSON::ByteBuffer.new, validating_keys = BSON::Config.validating_keys?)
      as_json.to_bson(buffer)
    end

    module ClassMethods

      # Deserialize the hash from BSON, converting to a DBRef if appropriate.
      #
      # @param [ String ] buffer The bson representing a hash.
      #
      # @return [ Hash, DBRef ] The decoded hash or DBRef.
      #
      # @see http://bsonspec.org/#/specification
      #
      # @since 2.0.0
      def from_bson(buffer, **options)
        # bson-ruby 4.8.0 changes #from_bson API to take **options.
        # However older bsons fail if invoked with a plain super here,
        # even if options are empty.
        decoded = if options.empty?
          super(buffer)
        else
          super
        end
        if ref = decoded[COLLECTION]
          decoded = DBRef.new(ref, decoded[ID], decoded[DATABASE])
        end
        decoded
      end
    end
  end

  ::Hash.send(:extend, DBRef::ClassMethods)
end

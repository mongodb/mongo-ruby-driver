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

require 'mongo/collection/view'

module Mongo

  # Represents a collection in the database and operations that can directly be
  # applied to one.
  #
  # @since 2.0.0
  class Collection
    extend Forwardable

    # @return [ Mongo::Database ] The database the collection resides in.
    attr_reader :database

    # @return [ String ] The name of the collection.
    attr_reader :name

    # @return [ Hash ] The collection options.
    attr_reader :options

    # Get client, cluser and server preference from client.
    def_delegators :@database, :client, :cluster, :server_preference, :write_concern

    # Delegate to the cluster for the next primary.
    def_delegators :cluster, :next_primary

    # Check if a collection is equal to another object. Will check the name and
    # the database for equality.
    #
    # @example Check collection equality.
    #   collection == other
    #
    # @param [ Object ] other The object to check.
    #
    # @return [ true, false ] If the objects are equal.
    #
    # @since 2.0.0
    def ==(other)
      return false unless other.is_a?(Collection)
      name == other.name && database == other.database && options == other.options
    end

    # Is the collection capped?
    #
    # @example Is the collection capped?
    #   collection.capped?
    #
    # @return [ true, false ] If the collection is capped.
    #
    # @since 2.0.0
    def capped?
      database.command(:collstats => name).documents[0]['capped']
    end

    # Force the collection to be created in the database.
    #
    # @example Force the collection to be created.
    #   collection.create
    #
    # @return [ Response ] The result of the command.
    #
    # @since 2.0.0
    def create
      database.command({ :create => name }.merge(options))
    end

    # Drop the collection. Will also drop all indexes associated with the
    # collection.
    #
    # @example Drop the collection.
    #   collection.drop
    #
    # @return [ Response ] The result of the command.
    #
    # @since 2.0.0
    def drop
      database.command(:drop => name)
    end

    # Find documents in the collection.
    #
    # @example Find documents in the collection by a selector.
    #   collection.find(name: 1)
    #
    # @example Get all documents in a collection.
    #   collection.find
    #
    # @param [ Hash ] selector The selector to use in the find.
    #
    # @return [ CollectionView ] The collection view.
    #
    # @since 2.0.0
    def find(selector = nil)
      View.new(self, selector || {})
    end

    # Get a view of all indexes for this collection. Can be iterated or has
    # more operations.
    #
    # @example Get the index view.
    #   collection.indexes
    #
    # @return [ View::Index ] The index view.
    #
    # @since 2.0.0
    def indexes
      Index::View.new(self)
    end

    # Instantiate a new collection.
    #
    # @example Instantiate a new collection.
    #   Mongo::Collection.new(database, 'test')
    #
    # @param [ Mongo::Database ] database The collection's database.
    # @param [ String, Symbol ] name The collection name.
    # @param [ Hash ] options The collection options.
    #
    # @since 2.0.0
    def initialize(database, name, options = {})
      raise InvalidName.new unless name
      @database = database
      @name = name.to_s.freeze
      @options = options
    end

    # Insert a single document into the collection.
    #
    # @example Insert a document into the collection.
    #   collection.insert_one({ name: 'test' })
    #
    # @param [ Hash ] document The document to insert.
    # @param [ Hash ] options The insert options.
    #
    # @return [ Response ] The database response wrapper.
    #
    # @since 2.0.0
    def insert_one(document, options = {})
      insert_many([ document ], options)
    end

    # Insert the provided documents into the collection.
    #
    # @example Insert documents into the collection.
    #   collection.insert_many([{ name: 'test' }])
    #
    # @param [ Array<Hash> ] documents The documents to insert.
    # @param [ Hash ] options The insert options.
    #
    # @return [ Response ] The database response wrapper.
    #
    # @since 2.0.0
    def insert_many(documents, options = {})
      Operation::Write::Insert.new(
        :documents => documents,
        :db_name => database.name,
        :coll_name => name,
        :write_concern => write_concern,
        :options => options
      ).execute(next_primary.context)
    end

    # Get the fully qualified namespace of the collection.
    #
    # @example Get the fully qualified namespace.
    #   collection.namespace
    #
    # @return [ String ] The collection namespace.
    #
    # @since 2.0.0
    def namespace
      "#{name}.#{database.name}"
    end

    # Exception that is raised when trying to create a collection with no name.
    #
    # @since 2.0.0
    class InvalidName < DriverError

      # The message is constant.
      #
      # @since 2.0.0
      MESSAGE = 'nil is an invalid collection name. ' +
        'Please provide a string or symbol.'

      # Instantiate the new exception.
      #
      # @example Instantiate the exception.
      #   Mongo::Collection::InvalidName.new
      #
      # @since 2.0.0
      def initialize
        super(MESSAGE)
      end
    end
  end
end

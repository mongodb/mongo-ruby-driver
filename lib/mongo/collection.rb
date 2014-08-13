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

require 'mongo/indexable'

module Mongo

  # Represents a collection in the database and operations that can directly be
  # applied to one.
  #
  # @since 2.0.0
  class Collection
    extend Forwardable
    include Indexable

    # @return [ Mongo::Database ] The database the collection resides in.
    attr_reader :database

    # @return [ String ] The name of the collection.
    attr_reader :name

    # Get client, cluser and server preference from client.
    def_delegators :@database, :client, :cluster, :server_preference, :write_concern

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
      name == other.name && database == other.database
    end

    # Instantiate a new collection.
    #
    # @example Instantiate a new collection.
    # Mongo::Collection.new(database, 'test')
    #
    # @param [ Mongo::Database ] database The collection's database.
    # @param [ String, Symbol ] name The collection name.
    #
    # @since 2.0.0
    def initialize(database, name)
      raise InvalidName.new unless name
      @database = database
      @name = name.to_s
    end

    # Insert the provided documents into the collection.
    #
    # @example Insert documents into the collection.
    # collection.insert([{ name: 'test' }])
    #
    # @param [ Array<Hash> ] documents The documents to insert.
    # @param [ Hash ] options The insert options.
    #
    # @return [ Mongo::Operation::Response ] The database response wrapper.
    #
    # @since 2.0.0
    def insert(documents, options = {})
      server = server_preference.primary(cluster.servers).first
      Operation::Write::Insert.new(
        :documents => documents.is_a?(Array) ? documents : [ documents ],
        :db_name => database.name,
        :coll_name => name,
        :write_concern => write_concern,
        :opts => options
      ).execute(server.context)
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

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

require 'mongo/collection/view/builder'
require 'mongo/collection/view/immutable'
require 'mongo/collection/view/iterable'
require 'mongo/collection/view/explainable'
require 'mongo/collection/view/aggregation'
require 'mongo/collection/view/map_reduce'
require 'mongo/collection/view/readable'
require 'mongo/collection/view/writable'

module Mongo
  class Collection

    # Representation of a query and options producing a result set of documents.
    #
    # A +View+ can be modified using helpers. Helpers can be chained,
    # as each one returns a +View+ if arguments are provided.
    #
    # The query message is sent to the server when a "terminator" is called.
    # For example, when #each is called on a +View+, a Cursor object is
    # created, which then sends the query to the server.
    #
    # A +View+ is not created directly by a user. Rather, +View+
    # creates a +View+ when a CRUD operation is called and returns it to
    # the user to interact with.
    #
    # @note The +View+ API is semipublic.
    # @api semipublic
    class View
      extend Forwardable
      include Enumerable
      include Immutable
      include Iterable
      include Readable
      include Retryable
      include Explainable
      include Writable

      # @return [ Collection ] The +Collection+ to query.
      attr_reader :collection

      # @return [ Hash ] The query filter.
      attr_reader :filter

      # Delegate necessary operations to the collection.
      def_delegators :collection, :client, :cluster, :database, :read_preference, :write_concern

      # Delegate to the cluster for the next primary.
      def_delegators :cluster, :next_primary

      alias :selector :filter

      # Compare two +View+ objects.
      #
      # @example Compare the view with another object.
      #   view == other
      #
      # @return [ true, false ] Equal if collection, filter, and options of two
      #   +View+ match.
      #
      # @since 2.0.0
      def ==(other)
        return false unless other.is_a?(View)
        collection == other.collection &&
            filter == other.filter &&
            options == other.options
      end
      alias_method :eql?, :==

      # A hash value for the +View+ composed of the collection namespace,
      # hash of the options and hash of the filter.
      #
      # @example Get the hash value.
      #   view.hash
      #
      # @return [ Integer ] A hash value of the +View+ object.
      #
      # @since 2.0.0
      def hash
        [ collection.namespace, options.hash, filter.hash ].hash
      end

      # Creates a new +View+.
      #
      # @example Find all users named Emily.
      #   View.new(collection, {:name => 'Emily'})
      #
      # @example Find all users named Emily skipping 5 and returning 10.
      #   View.new(collection, {:name => 'Emily'}, :skip => 5, :limit => 10)
      #
      # @example Find all users named Emily using a specific read preference.
      #   View.new(collection, {:name => 'Emily'}, :read => :secondary_preferred)
      #
      # @param [ Collection ] collection The +Collection+ to query.
      # @param [ Hash ] filter The query filter.
      # @param [ Hash ] options The additional query options.
      #
      # @option options :comment [ String ] Associate a comment with the query.
      # @option options :batch_size [ Integer ] The number of docs to return in
      #   each response from MongoDB.
      # @option options :fields [ Hash ] The fields to include or exclude in
      #   returned docs.
      # @option options :hint [ Hash ] Override default index selection and force
      #   MongoDB to use a specific index for the query.
      # @option options :limit [ Integer ] Max number of docs to return.
      # @option options :max_scan [ Integer ] Constrain the query to only scan the
      #   specified number of docs. Use to prevent queries from running too long.
      # @option options :read [ Symbol ] The read preference to use for the query.
      #   If none is provided, the collection's default read preference is used.
      # @option options :show_disk_loc [ true, false ] Return disk location info as
      #   a field in each doc.
      # @option options :skip [ Integer ] The number of documents to skip.
      # @option options :snapshot [ true, false ] Prevents returning a doc more than
      #   once.
      # @option options :sort [ Hash ] The key and direction pairs used to sort the
      #   results.
      #
      # @since 2.0.0
      def initialize(collection, filter = {}, options = {})
        validate_doc!(filter)
        @collection = collection
        parse_parameters!(BSON::Document.new(filter.freeze), BSON::Document.new(options.freeze))
      end

      # Get a human-readable string representation of +View+.
      #
      # @example Get the inspection.
      #   view.inspect
      #
      # @return [ String ] A string representation of a +View+ instance.
      #
      # @since 2.0.0
      def inspect
        "#<Mongo::Collection::View:0x#{object_id} namespace='#{collection.namespace}'" +
            " @filter=#{filter.to_s} @options=#{options.to_s}>"
      end

      private

      def initialize_copy(other)
        @collection = other.collection
        @options = other.options.dup
        @filter = other.filter.dup
      end

      def parse_parameters!(filter, options)
        query = filter.delete(QUERY)
        modifiers = (filter || {}).merge(options.delete(MODIFIERS) || {})
        @filter = query || filter
        @options = Builder::Modifiers.map_driver_options(modifiers).merge!(options)
      end

      def new(options)
        View.new(collection, filter, options)
      end

      def view; self; end
    end
  end
end

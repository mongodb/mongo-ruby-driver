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

require 'mongo/collection/view/builder'
require 'mongo/collection/view/immutable'
require 'mongo/collection/view/iterable'
require 'mongo/collection/view/explainable'
require 'mongo/collection/view/aggregation'
require 'mongo/collection/view/change_stream'
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
      include Explainable
      include Writable

      # @return [ Collection ] The +Collection+ to query.
      attr_reader :collection

      # @return [ Hash ] The query filter.
      attr_reader :filter

      # Delegate necessary operations to the collection.
      def_delegators :collection,
                     :client,
                     :cluster,
                     :database,
                     :read_with_retry,
                     :read_with_retry_cursor,
                     :write_with_retry,
                     :nro_write_with_retry,
                     :write_concern_with_session

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
      # @option options [ true, false ] :allow_disk_use When set to true, the
      #   server can write temporary data to disk while executing the find
      #   operation. This option is only available on MongoDB server versions
      #   4.4 and newer.
      # @option options [ Integer ] :batch_size The number of documents to
      #   return in each response from MongoDB.
      # @option options [ Hash ] :collation The collation to use.
      # @option options [ String ] :comment Associate a comment with the query.
      # @option options [ :tailable, :tailable_await ] :cursor_type The type of cursor to use.
      # @option options [ Hash ] :explain Execute an explain with the provided
      #   explain options (known options are :verbose and :verbosity) rather
      #   than a find.
      # @option options [ Hash ] :hint Override the default index selection and
      #   force MongoDB to use a specific index for the query.
      # @option options [ Integer ] :limit Max number of documents to return.
      # @option options [ Integer ] :max_scan Constrain the query to only scan
      #   the specified number of documents. Use to prevent queries from
      #   running for too long. Deprecated as of MongoDB server version 4.0.
      # @option options [ Hash ] :projection The fields to include or exclude
      #   in the returned documents.
      # @option options [ Hash ] :read The read preference to use for the
      #   query. If none is provided, the collection's default read preference
      #   is used.
      # @option options [ Hash ] :read_concern The read concern to use for
      #   the query.
      # @option options [ true | false ] :show_disk_loc Return disk location
      #   info as a field in each doc.
      # @option options [ Integer ] :skip The number of documents to skip.
      # @option options [ true | false ] :snapshot Prevents returning a
      #   document more than once. Deprecated as of MongoDB server version 4.0.
      # @option options [ Hash ] :sort The key and direction pairs used to sort
      #   the results.
      #
      # @since 2.0.0
      def initialize(collection, filter = {}, options = {})
        validate_doc!(filter)
        @collection = collection

        filter = BSON::Document.new(filter)
        options = BSON::Document.new(options)

        # This is when users pass $query in filter and other modifiers
        # alongside?
        query = filter.delete(:$query)
        # This makes modifiers contain the filter if filter wasn't
        # given via $query but as top-level keys, presumably
        # downstream code ignores non-modifier keys in the modifiers?
        modifiers = filter.merge(options.delete(:modifiers) || {})
        @filter = (query || filter).freeze
        @options = Operation::Find::Builder::Modifiers.map_driver_options(modifiers).merge!(options).freeze
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

      # Get the write concern on this +View+.
      #
      # @example Get the write concern.
      #   view.write_concern
      #
      # @return [ Mongo::WriteConcern ] The write concern.
      #
      # @since 2.0.0
      def write_concern
        WriteConcern.get(options[:write_concern] || options[:write] || collection.write_concern)
      end

      private

      def initialize_copy(other)
        @collection = other.collection
        @options = other.options.dup
        @filter = other.filter.dup
      end

      def new(options)
        View.new(collection, filter, options)
      end

      def view; self; end

      def with_session(opts = {}, &block)
        client.send(:with_session, @options.merge(opts), &block)
      end
    end
  end
end

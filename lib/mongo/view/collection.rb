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

    # Representation of a query and options producing a result set of documents.
    #
    # A +Collection+ can be modified using helpers.  Helpers can be chained,
    # as each one returns a +Collection+ if arguments are provided.
    #
    # The query message is sent to the server when a "terminator" is called.
    # For example, when #each is called on a +Collection+, a Cursor object is
    # created, which then sends the query to the server.
    #
    # A +Collection+ is not created directly by a user. Rather, +Collection+
    # creates a +Collection+ when a CRUD operation is called and returns it to
    # the user to interact with.
    #
    # @note The +Collection+ API is semipublic.
    # @api semipublic
    class Collection
      extend Forwardable
      include Enumerable
      include Executable

      # @return [ Collection ] The +Collection+ to query.
      attr_reader :collection
      # @return [ Hash ] The query selector.
      attr_reader :selector
      # @return [ Hash ] The additional query options.
      attr_reader :options

      def_delegators :@collection, :client, :cluster, :database, :server_preference, :write_concern

      # Compare two +Collection+ objects.
      #
      # @example Compare the view with another object.
      #   view == other
      #
      # @return [ true, false ] Equal if collection, selector, and options of two
      #   +Collection+ match.
      #
      # @since 2.0.0
      def ==(other)
        return false unless other.is_a?(Collection)
        collection == other.collection &&
            selector == other.selector &&
            options == other.options
      end
      alias_method :eql?, :==

      # Creates a new +Collection+.
      #
      # @example Find all users named Emily.
      #   Collection.new(collection, {:name => 'Emily'})
      #
      # @example Find all users named Emily skipping 5 and returning 10.
      #   Collection.new(collection, {:name => 'Emily'}, :skip => 5, :limit => 10)
      #
      # @example Find all users named Emily using a specific read preference.
      #   Collection.new(collection, {:name => 'Emily'}, :read => :secondary_preferred)
      #
      # @param [ Collection ] collection The +Collection+ to query.
      # @param [ Hash ] selector The query selector.
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
      def initialize(collection, selector = {}, options = {})
        @collection = collection
        @selector = selector.dup
        @options = options.dup
      end

      # Get a human-readable string representation of +Collection+.
      #
      # @example Get the inspection.
      #   view.inspect
      #
      # @return [ String ] A string representation of a +Collection+ instance.
      #
      # @since 2.0.0
      def inspect
        "<Mongo::View::Collection:0x#{object_id} namespace='#{collection.namespace}" +
            " @selector=#{selector.inspect} @options=#{options.inspect}>"
      end

      # A hash value for the +Collection+ composed of the collection namespace,
      # hash of the options and hash of the selector.
      #
      # @example Get the hash value.
      #   view.hash
      #
      # @return [ Integer ] A hash value of the +Collection+ object.
      #
      # @since 2.0.0
      def hash
        [ collection.namespace, options.hash, selector.hash ].hash
      end

      # Associate a comment with the query.
      #
      # @example Add a comment.
      #   view.comment('slow query')
      #
      # @note Set profilingLevel to 2 and the comment will be logged in the profile
      #   collection along with the query.
      #
      # @param [ String ] comment The comment to be associated with the query.
      #
      # @return [ String, Collection ] Either the comment or a
      #   new +Collection+.
      #
      # @since 2.0.0
      def comment(comment = nil)
        set_option(:comment, comment)
      end

      # The number of documents returned in each batch of results from MongoDB.
      # Specifying 1 or a negative number is analogous to setting a limit.
      #
      # @param [ Integer ] batch_size The size of each batch of results.
      #
      # @return [ Integer, Collection ] Either the batch_size value or a
      # new +Collection+.
      def batch_size(batch_size = nil)
        set_option(:batch_size, batch_size)
      end

      # The fields to include or exclude from each doc in the result set.
      # A value of 0 excludes a field from the doc. A value of 1 includes it.
      # Values must all be 0 or all be 1, with the exception of the _id value.
      # The _id field is included by default. It must be excluded explicitly.
      #
      # @param [ Hash ] fields The field and 1 or 0, to include or exclude it.
      #
      # @return [ Collection ] Either the fields or a new +Collection+.
      def fields(fields = nil)
        set_option(:fields, fields)
      end

      # The index that MongoDB will be forced to use for the query.
      #
      # @param [ Hash ] hint The index to use for the query.
      #
      # @return [ Hash, Collection ] Either the hint or a new +Collection+.
      def hint(hint = nil)
        set_option(:hint, hint)
      end

      # The max number of docs to return from the query.
      #
      # @param [ Integer ] limit The number of docs to return.
      #
      # @return [ Integer, Collection ] Either the limit or a new +Collection+.
      def limit(limit = nil)
        set_option(:limit, limit)
      end

      # The read preference to use for the query.
      # If none is specified for the query, the read preference of the
      # collection will be used.
      #
      # @param [ Hash ] read The read preference mode to use for the query.
      #
      # @return [ Symbol, Collection ] Either the read preference or a
      # new +Collection+.
      def read(read = nil)
        return default_read if read.nil?
        set_option(:read, read)
      end

      # The number of docs to skip before returning results.
      #
      # @param [ Integer ] skip Number of docs to skip.
      #
      # @return [ Integer, Collection ] Either the skip value or a
      # new +Collection+.
      def skip(skip = nil)
        set_option(:skip, skip)
      end

      # The key and direction pairs by which the result set will be sorted.
      #
      # @param [ Hash ] sort The attributes and directions to sort by.
      #
      # @return [ Hash, Collection ] Either the sort setting or a
      # new +Collection+.
      def sort(sort = nil)
        set_option(:sort, sort)
      end

      # Set options for the query.
      #
      # @param s_options [ Hash ] Special query options.
      #
      # @option s_options :snapshot [ true, false ] Prevents returning docs more
      #   than once.
      # @option s_options :max_scan [ Integer ] Constrain the query to only scan the
      #   specified number of docs.
      # @option s_options :show_disk_loc [ true, false ] Return disk location info
      #   as a field in each doc.
      #
      # @return [ Hash, Collection ] Either the special query options or a
      # new +Collection+.
      def special_options(s_options = nil)
        return special_options_hash if s_options.nil?
        opts = options.dup
        [:snapshot, :max_scan, :show_disk_loc, :explain].each do |k|
          s_options[k].nil? ? opts.delete(k) : opts.merge!(k => s_options[k])
        end
        Collection.new(collection, selector, opts)
      end

      # Iterate through documents returned by a query with this +Collection+.
      #
      # @example Iterate through the result of the view.
      #   view.each do |document|
      #     p document
      #   end
      #
      # @return [ Enumerator ] The enumerator.
      #
      # @since 2.0.0
      #
      # @yieldparam [ Hash ] Each matching document.
      def each
        server = read.select_servers(cluster.servers).first
        cursor = Cursor.new(self, send_initial_query(server), server).to_enum
        cursor.each do |doc|
          yield doc
        end if block_given?
        cursor
      end

      private

      SPECIAL_FIELDS = [
          [:$query,          :selector],
          [:$readPreference, :read_pref_formatted],
          [:$orderby,        :sort],
          [:$hint,           :hint],
          [:$comment,        :comment],
          [:$snapshot,       :snapshot],
          [:$maxScan,        :max_scan],
          [:$showDiskLoc,    :show_disk_loc],
          [:$explain,        :explain_value]
      ]

      def explain_value
        special_options[:explain]
      end

      def snapshot
        special_options[:snapshot]
      end

      def max_scan
        special_options[:max_scan]
      end

      def show_disk_loc
        special_options[:show_disk_loc]
      end

      def initial_query_op
        Operation::Read::Query.new(query_spec)
      end

      def send_initial_query(server)
        # @todo: if mongos, don't send read pref because it's
        # in the special selector
        initial_query_op.execute(server.context)
      end

      def read_pref_formatted
        read.to_mongos
      end

      def special_selector
        SPECIAL_FIELDS.reduce({}) do |hash, pair|
          key, method = pair
          value = send(method)
          hash[key] = value if value
          hash
        end
      end

      # Get a hash of the query options.
      #
      # @return [Hash] The query options.
      # @todo: refactor this? it knows too much about the query wire protocol
      # message interface
      def query_options
        { :project => fields,
          :skip   => skip,
          :limit  => to_return,
          :flags  => flags }
      end

      # The flags set on this query.
      #
      # @return [Array] List of flags to be set on the query message.
      # @todo: add no_cursor_timeout option
      def flags
        flags << :slave_ok if need_slave_ok?
      end

      def has_special_fields?
        !special_options.empty? || sort || hint || comment || cluster.sharded?
      end

      def initialize_copy(other)
        @collection = other.collection
        @options = other.options.dup
        @selector = other.selector.dup
      end

      def default_read(read = nil)
        options[:read] || server_preference
      end

      def special_options_hash
        s_options = options[:snapshot].nil? ? {} : { :snapshot => options[:snapshot] }
        unless options[:max_scan].nil?
          s_options[:max_scan] = options[:max_scan]
        end
        unless options[:show_disk_loc].nil?
          s_options[:show_disk_loc] = @options[:show_disk_loc]
        end
        unless options[:explain].nil?
          s_options[:explain] = options[:explain]
        end
        s_options
      end

      def query_spec
        sel = has_special_fields? ? special_selector : selector
        { :selector  => sel,
          :options      => query_options,
          :db_name   => db_name,
          :coll_name => @collection.name }
      end

      def primary?
        read.name == :primary
      end

      def need_slave_ok?
        !primary?
      end

      def to_return
        [ limit || batch_size, batch_size || limit ].min
      end

      def db_name
        collection.database.name
      end

      def set_option(field, value)
        return options[field] if value.nil?
        Collection.new(collection, selector, options.merge(field => value))
      end
    end
  end
end

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

  # Representation of a query and options producing a result set of documents.
  #
  # A +CollectionView+ can be modified using helpers.  Helpers can be chained,
  # as each one returns a +CollectionView+ if arguments are provided.
  #
  # The query message is sent to the server when a "terminator" is called.
  # For example, when #each is called on a +CollectionView+, a Cursor object is
  # created, which then sends the query to the server.
  #
  # A +CollectionView+ is not created directly by a user. Rather, +Collection+
  # creates a +CollectionView+ when a CRUD operation is called and returns it to
  # the user to interact with.
  #
  # @note The +CollectionView+ API is semipublic.
  # @api semipublic
  class CollectionView
    include Enumerable

    # @return [ Collection ] The +Collection+ to query.
    attr_reader :collection
    # @return [ Hash ] The query selector.
    attr_reader :selector
    # @return [ Hash ] The additional query options.
    attr_reader :opts

    # Creates a new +CollectionView+.
    #
    # @example Find all users named Emily.
    #   CollectionView.new(collection, {:name => 'Emily'})
    #
    # @example Find all users named Emily skipping 5 and returning 10.
    #   CollectionView.new(collection, {:name => 'Emily'}, :skip => 5, :limit => 10)
    #
    # @example Find all users named Emily using a specific read preference.
    #   CollectionView.new(collection, {:name => 'Emily'}, :read => :secondary_preferred)
    #
    # @param [ Collection ] collection The +Collection+ to query.
    # @param [ Hash ] selector The query selector.
    # @param [ Hash ] opts The additional query options.
    #
    # @option opts :comment [ String ] Associate a comment with the query.
    # @option opts :batch_size [ Integer ] The number of docs to return in
    #   each response from MongoDB.
    # @option opts :fields [ Hash ] The fields to include or exclude in
    #   returned docs.
    # @option opts :hint [ Hash ] Override default index selection and force
    #   MongoDB to use a specific index for the query.
    # @option opts :limit [ Integer ] Max number of docs to return.
    # @option opts :max_scan [ Integer ] Constrain the query to only scan the
    #   specified number of docs. Use to prevent queries from running too long.
    # @option opts :read [ Symbol ] The read preference to use for the query.
    #   If none is provided, the collection's default read preference is used.
    # @option opts :show_disk_loc [ true, false ] Return disk location info as
    #   a field in each doc.
    # @option opts :skip [ Integer ] The number of documents to skip.
    # @option opts :snapshot [ true, false ] Prevents returning a doc more than
    #   once.
    # @option opts :sort [ Hash ] The key and direction pairs used to sort the
    #   results.
    def initialize(collection, selector = {}, opts = {})
      @collection = collection
      @selector = selector.dup
      @opts = opts.dup
    end

    # Get a human-readable string representation of +CollectionView+.
    #
    # @return [ String ] A string representation of a +CollectionView+ instance.
    def inspect
      "<Mongo::CollectionView:0x#{object_id} namespace='#{@collection.full_namespace}" +
          " @selector=#{@selector.inspect} @opts=#{@opts.inspect}>"
    end

    # Compare two +CollectionView+ objects.
    #
    # @return [ true, false ] Equal if collection, selector, and opts of two
    #   +CollectionView+ match.
    def ==(other)
      @collection == other.collection &&
          @selector == other.selector &&
          @opts == other.opts
    end
    alias_method :eql?, :==

    # A hash value for the +CollectionView+ composed of the collection namespace,
    # hash of the options and hash of the selector.
    #
    # @return [ Integer ] A hash value of the +CollectionView+ object.
    def hash
      [@collection.full_namespace, @opts.hash, @selector.hash].hash
    end

    # Get the size of the result set for the query.
    #
    # @return [ Integer ] The number of documents in the result set.
    def count
      cmd = { :count => @collection.name,
        :query => @selector,
        :limit => limit,
        :skip  => skip,
        :hint  => hint }
      @collection.database.command(cmd)
    end

    # Get the explain plan for the query.
    #
    # @return [ Hash ] A single document with the explain plan.
    def explain
      explain_limit = limit || 0
      opts = @opts.merge(:limit => -explain_limit.abs, :explain => true)
      @collection.explain(CollectionView.new(@collection, @selector, opts))
    end

    # Get the distinct values for a specified field across a single
    # collection.
    # Note that if a @selector is defined, it will be used in the analysis.
    #
    # @param [ Symbol, String ] key The field to collect distinct values from.
    #
    # @return [ Hash ] A doc with an array of the distinct values and query plan.
    def distinct(key)
      @collection.distinct(self, key)
    end

    # Associate a comment with the query.
    # Set profilingLevel to 2 and the comment will be logged in the profile
    # collection along with the query.
    #
    # @param [ String ] comment The comment to be associated with the query.
    #
    # @return [ String, CollectionView ] Either the comment or a
    #   new +CollectionView+.
    def comment(comment = nil)
      set_option(:comment, comment)
    end

    # The number of documents returned in each batch of results from MongoDB.
    # Specifying 1 or a negative number is analogous to setting a limit.
    #
    # @param [ Integer ] batch_size The size of each batch of results.
    #
    # @return [ Integer, CollectionView ] Either the batch_size value or a
    # new +CollectionView+.
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
    # @return [ CollectionView ] Either the fields or a new +CollectionView+.
    def fields(fields = nil)
      set_option(:fields, fields)
    end

    # The index that MongoDB will be forced to use for the query.
    #
    # @param [ Hash ] hint The index to use for the query.
    #
    # @return [ Hash, CollectionView ] Either the hint or a new +CollectionView+.
    def hint(hint = nil)
      set_option(:hint, hint)
    end

    # The max number of docs to return from the query.
    #
    # @param [ Integer ] limit The number of docs to return.
    #
    # @return [ Integer, CollectionView ] Either the limit or a new +CollectionView+.
    def limit(limit = nil)
      set_option(:limit, limit)
    end

    # The read preference to use for the query.
    # If none is specified for the query, the read preference of the
    # collection will be used.
    #
    # @param [ Symbol ] read The read preference to use for the query.
    #
    # @return [ ServerPreference, CollectionView ] Either the read preference or a
    # new +CollectionView+.
    def read(read = nil)
      return default_read if read.nil?
      set_option(:read, read)
    end

    # The number of docs to skip before returning results.
    #
    # @param [ Integer ] skip Number of docs to skip.
    #
    # @return [ Integer, CollectionView ] Either the skip value or a
    # new +CollectionView+.
    def skip(skip = nil)
      set_option(:skip, skip)
    end

    # The key and direction pairs by which the result set will be sorted.
    #
    # @param [ Hash ] sort The attributes and directions to sort by.
    #
    # @return [ Hash, CollectionView ] Either the sort setting or a
    # new +CollectionView+.
    def sort(sort = nil)
      set_option(:sort, sort)
    end

    # Set options for the query.
    #
    # @param s_opts [ Hash ] Special query options.
    #
    # @option s_opts :snapshot [ true, false ] Prevents returning docs more
    #   than once.
    # @option s_opts :max_scan [ Integer ] Constrain the query to only scan the
    #   specified number of docs.
    # @option s_opts :show_disk_loc [ true, false ] Return disk location info
    #   as a field in each doc.
    #
    # @return [ Hash, CollectionView ] Either the special query options or a
    # new +CollectionView+.
    def special_opts(s_opts = nil)
      return special_opts_hash if s_opts.nil?
      opts = @opts.dup
      [:snapshot, :max_scan, :show_disk_loc].each do |k|
        s_opts[k].nil? ? opts.delete(k) : opts.merge!(k => s_opts[k])
      end
      CollectionView.new(collection, selector, opts)
    end

    # Iterate through documents returned by a query with this +CollectionView+.
    #
    # @return [ Enumerator ] The enumerator.
    #
    # @yieldparam [ Hash ] Each matching document.
    def each
      cursor = Cursor.new(self, send_initial_query, @context).to_enum
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
        [:$showDiskLoc,    :show_disk_loc]
    ]

    # The snapshot special operator.
    #
    # @return [true, false, nil]
    def snapshot
      special_opts[:snapshot]
    end

    # The max_scan special operator.
    #
    # @return [Integer, nil]
    def max_scan
      special_opts[:max_scan]
    end

    # The show_disk_loc special operator.
    #
    # @return [true, false, nil]
    def show_disk_loc
      special_opts[:show_disk_loc]
    end

    # The initial query operation to send to the server.
    #
    def initial_query_op
      Mongo::Operation::Read::Query.new(query_spec)
    end

    # Send the initial query operation to the server.
    #
    # @return [ Mongo::Response ] The initial query response.
    def send_initial_query
      # @todo: if mongos, don't send read pref because it's
      # in the special selector
      # @todo - use read.server, when implemented, not just primary.
      @context = read.primary(@collection.cluster.servers).first.context
      initial_query_op.execute(@context)
    end

    # Get the read preference for this query.
    #
    # @return [Hash, nil] The read preference or nil.
    def read_pref_formatted
      read.to_mongos
    end

    # Build a special query selector.
    #
    # @return [Hash] The special query selector.
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
    def query_opts
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

    # Determine whether this query has special fields.
    #
    # @return [true, false] Whether the query has special fields.
    def has_special_fields?
      (!special_opts.empty? || sort || hint || comment )
      # @todo - we used to check @collection.client.mongos? here?
    end

    # Clone or dup the current +CollectionView+.
    #
    # The @opt and @selector instance variables are duped and the
    # +Collection+ reference remains intact.
    #
    # @param [ CollectionView ] other The +CollectionView+ to be cloned.
    #
    # @return [ CollectionView ] The new +CollectionView+.
    def initialize_copy(other)
      @collection = other.collection
      @opts = other.opts.dup
      @selector = other.selector.dup
    end

    # The read preference for this operation.
    #
    # @return [ ServerPreference ] This operation's server preference.
    def default_read(read = nil)
      if @opts[:read]
        ServerPreference.get(@opts[:read])
      else
        @collection.server_preference
      end
    end

    # Extract query opts from @opts and return them in a separate hash.
    #
    # @return [ Hash ] The query options in their own hash.
    def special_opts_hash
      s_opts = @opts[:snapshot].nil? ? {} : { :snapshot => @opts[:snapshot] }
      unless @opts[:max_scan].nil?
        s_opts[:max_scan] = @opts[:max_scan]
      end
      unless @opts[:show_disk_loc].nil?
        s_opts[:show_disk_loc] = @opts[:show_disk_loc]
      end
      s_opts
    end

    # Build the query selector and initial +Query+ message.
    #
    # @return [Hash] The +Query+ operation spec.
    def query_spec
      sel = has_special_fields? ? special_selector : selector
      { :selector  => sel,
        :opts      => query_opts,
        :db_name   => db_name,
        :coll_name => @collection.name }
    end

    # Whether the read preference mode is primary.
    #
    # @return [true, false] Whether the read preference mode is primary.
    def primary?
      read.name == :primary
    end

    # Whether the slave ok bit needs to be set on the wire protocol message.
    #
    # @return [true, false] Whether the slave ok bit needs to be set.
    def need_slave_ok?
      !primary?
    end

    # The number of documents to return in the next batch.
    #
    # @return [Integer] The number of documents to return in the next batch.
    def to_return
      [limit || batch_size, batch_size || limit].min
    end

    # The name of the database containing the queried collection.
    #
    # @return [String] The database name.
    def db_name
      @collection.database.name
    end

    # Either return the option value or create a new +CollectionView+ with
    # the option value set.
    #
    # @return [ Object, CollectionView ] Either the option value or a
    # new +CollectionView+.
    def set_option(field, value)
      return @opts[field] if value.nil?
      CollectionView.new(collection, selector, @opts.merge(field => value))
    end

    # Set the option value on this +CollectionView+.
    #
    # @return [ CollectionView ] self.
    def mutate(field, value)
      @opts.merge!(field => value) unless value.nil?
      self
    end
  end
end

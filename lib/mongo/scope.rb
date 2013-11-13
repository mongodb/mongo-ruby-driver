# Copyright (C) 2009-2013 MongoDB, Inc.
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
  # A +Scope+ can be modified using helpers.  Helpers can be chained, as each
  # one returns a +Scope+ if arguments are provided.
  #
  # The query message is sent to the server when a "terminator" is called.
  # For example, when #each is called on a +Scope+, a Cursor object is created,
  # which then sends the query to the server.
  #
  # A +Scope+ is not created directly by a user. Rather, +Collection+ creates a
  # +Scope+ when a CRUD operation is called and returns it to the user to
  # interact with.
  #
  # @note The +Scope+ API is semipublic.
  # @api semipublic
  class Scope
    include Enumerable

    # @return [Collection] The +Collection+ to query.
    attr_reader :collection
    # @return [Hash] The query selector.
    attr_reader :selector
    # @return [Hash] The additional query options.
    attr_reader :opts

    # Creates a new +Scope+.
    #
    # @example Find all users named Emily.
    #   Scope.new(collection, {:name => 'Emily'})
    #
    # @example Find all users named Emily skipping 5 and returning 10.
    #   Scope.new(collection, {:name => 'Emily'}, :skip => 5, :limit => 10)
    #
    # @example Find all users named Emily using a specific read preference.
    #   Scope.new(collection, {:name => 'Emily'}, :read => :secondary_preferred)
    #
    # @param collection [Collection] The +Collection+ to query.
    # @param selector [Hash] The query selector.
    # @param opts [Hash] The additional query options.
    #
    # @option opts :comment [String] Associate a comment with the query.
    # @option opts :batch_size [Integer] The number of docs to return in
    #   each response from MongoDB.
    # @option opts :fields [Hash] The fields to include or exclude in
    #   returned docs.
    # @option opts :hint [Hash] Override default index selection and force
    #   MongoDB to use a specific index for the query.
    # @option opts :limit [Integer] Max number of docs to return.
    # @option opts :max_scan [Integer] Constrain the query to only scan the
    #   specified number of docs. Use to prevent queries from running too long.
    # @option opts :read [Symbol] The read preference to use for the query.
    #   If none is provided, the collection's default read preference is used.
    # @option opts :show_disk_loc [true, false] Return disk location info as
    #   a field in each doc.
    # @option opts :skip [Integer] The number of documents to skip.
    # @option opts :snapshot [true, false] Prevents returning a doc more than
    #   once.
    # @option opts :sort [Hash] The key and direction pairs used to sort the
    #   results.
    def initialize(collection, selector = {}, opts = {})
      @collection = collection
      @selector = selector.dup
      @opts = opts.dup
    end

    # Get a human-readable string representation of +Scope+.
    #
    # @return [String] A string representation of a +Scope+ instance.
    def inspect
      "<Mongo::Scope:0x#{object_id} namespace='#{@collection.full_namespace}" +
      " @selector=#{@selector.inspect} @opts=#{@opts.inspect}>"
    end

    # Get the size of the result set for the query.
    #
    # @return [Integer] The number of documents in the result set.
    def count
      @collection.count(Scope.new(@collection, @selector, @opts))
    end

    # Get the explain plan for the query.
    #
    # @return [Hash] A single document with the explain plan.
    def explain
      explain_limit = limit || 0
      opts = @opts.merge(:limit => -explain_limit.abs, :explain => true)
      @collection.explain(Scope.new(@collection, @selector, opts))
    end

    # Get the distinct values for a specified field across a single
    # collection.
    # Note that if a @selector is defined, it will be used in the anaylsis.
    #
    # @param key [Symbol, String] The field to collect distinct values from.
    #
    # @return [Hash] A doc with an array of the distinct values and query plan.
    def distinct(key)
      @collection.distinct(self, key)
    end

    # Associate a comment with the query.
    # Set profilingLevel to 2 and the comment will be logged in the profile
    # collection along with the query.
    #
    # @param comment [String] The comment to be associated with the query.
    #
    # @return [String, Scope] Either the comment or a new +Scope+.
    def comment(comment = nil)
      set_option(:comment, comment)
    end

    # Modify this +Scope+ to associate a comment with the query.
    #
    # @param comment [String] The comment to be associated with the query.
    #
    # @return [Scope] self.
    def comment!(comment = nil)
      mutate(:comment, comment)
    end

    # The number of documents returned in each batch of results from MongoDB.
    # Specifying 1 or a negative number is analogous to setting a limit.
    #
    # @param batch_size [Integer] The size of each batch of results.
    #
    # @return [Integer, Scope] Either the batch_size value or a new +Scope+.
    def batch_size(batch_size = nil)
      set_option(:batch_size, batch_size)
    end

    # Modify this +Scope+ to define the number of documents returned in each
    # batch of results from MongoDB.
    #
    # @param batch_size [Integer] The size of each batch of results.
    #
    # @return [Scope] self.
    def batch_size!(batch_size = nil)
      mutate(:batch_size, batch_size)
    end

    # The fields to include or exclude from each doc in the result set.
    # A value of 0 excludes a field from the doc. A value of 1 includes it.
    # Values must all be 0 or all be 1, with the exception of the _id value.
    # The _id field is included by default. It must be excluded explicitly.
    #
    # @param fields [Hash] The field and 1 or 0, to include or exclude it.
    #
    # @return [Scope] Either the fields or a new +Scope+.
    def fields(fields = nil)
      set_option(:fields, fields)
    end

    # Modify this +Scope+ to define the fields to include or exclude from each
    # doc in the result set.
    #
    # @param fields [Hash] The field and 1 or 0, to include or exclude it.
    #
    # @return [Scope] self.
    def fields!(fields = nil)
      mutate(:fields, fields)
    end

    # The index that MongoDB will be forced to use for the query.
    #
    # @param hint [Hash] The index to use for the query.
    #
    # @return [Hash, Scope] Either the hint or a new +Scope+.
    def hint(hint = nil)
      set_option(:hint, hint)
    end

    # Modify this +Scope+ to define the index that MongoDB will be forced
    # to use for the query.
    #
    # @param hint [Hash] The index to use for the query.
    #
    # @return [Scope] self.
    def hint!(hint = nil)
      mutate(:hint, hint)
    end

    # The max number of docs to return from the query.
    #
    # @param limit [Integer] The number of docs to return.
    #
    # @return [Integer, Scope] Either the limit or a new +Scope+.
    def limit(limit = nil)
      set_option(:limit, limit)
    end

    # Modify this +Scope+ to define the max number of docs to return from
    # the query.
    #
    # @param limit [Integer] The number of docs to return.
    #
    # @return [Scope] self.
    def limit!(limit = nil)
      mutate(:limit, limit)
    end

    # The read preference to use for the query.
    # If none is specified for the query, the read preference of the
    # collection will be used.
    #
    # @param read [Symbol] The read preference to use for the query.
    #
    # @return [Symbol, Scope] Either the read preference or a new +Scope+.
    def read(read = nil)
      return default_read if read.nil?
      set_option(:read, read)
    end

    # Modify this +Scope+ to define the read preference to use for the query.
    #
    # @param read [Symbol] The read preference to use for the query.
    #
    # @return [Scope] self.
    def read!(read = nil)
      mutate(:read, read)
    end

    # The number of docs to skip before returning results.
    #
    # @param skip [Integer] Number of docs to skip.
    #
    # @return [Integer, Scope] Either the skip value or a new +Scope+.
    def skip(skip = nil)
      set_option(:skip, skip)
    end

    # Modify this +Scope+ to define the number of docs to skip before returning
    # results.
    #
    # @param skip [Integer] Number of docs to skip.
    #
    # @return [Scope] self.
    def skip!(skip = nil)
      mutate(:skip, skip)
    end

    # The key and direction pairs by which the result set will be sorted.
    #
    # @param sort [Hash] The attributes and directions to sort by.
    #
    # @return [Hash, Scope] Either the sort setting or a new +Scope+.
    def sort(sort = nil)
      set_option(:sort, sort)
    end

    # Modify this +Scope+ to define the attributes by which the result set
    # will be sorted.
    #
    # @param sort [Hash] The attributes and directions to sort by.
    #
    # @return [Scope] self.
    def sort!(sort = nil)
      mutate(:sort, sort)
    end

    # Set options for the query.
    #
    # @param q_opts [Hash] Query options.
    #
    # @option q_opts :snapshot [true, false] Prevents returning docs more
    #   than once.
    # @option q_opts :max_scan [Integer] Constrain the query to only scan the
    #   specified number of docs.
    # @option q_opts :show_disk_loc [true, false] Return disk location info
    #   as a field in each doc.
    #
    # @return [Hash, Scope] Either the query options or a new +Scope+.
    def query_opts(q_opts = nil)
      return query_opts_hash if q_opts.nil?
      opts = @opts.dup
      [:snapshot, :max_scan, :show_disk_loc].each do |k|
        q_opts[k].nil? ? opts.delete(k) : opts.merge!(k => q_opts[k])
      end
      Scope.new(collection, selector, opts)
    end

    # Modify this +Scope+ to set options for the query.
    #
    # @param q_opts [Hash] Query options.
    #
    # @option q_opts :snapshot [true, false] Prevents returning docs more
    #   than once.
    # @option q_opts :max_scan [Integer] Constrain the query to only scan the
    #   specified number of docs.
    # @option q_opts :show_disk_loc [true, false] Return disk location info
    #   as a field in each doc.
    #
    # @return [Scope] self
    def query_opts!(q_opts = nil)
      return self if q_opts.nil?
      [:snapshot, :max_scan, :show_disk_loc].each do |k|
        q_opts[k].nil? ? @opts.delete(k) : @opts.merge!(k => q_opts[k])
      end
      self
    end

    # Compare two +Scope+ objects.
    #
    # @return [true, false] Equal if collection, selector, and opts of two
    #   +Scopes+ match.
    def ==(other)
      @collection == other.collection &&
        @selector == other.selector &&
        @opts == other.opts
    end
    alias_method :eql?, :==

    # A hash value for the +Scope+ composed of the collection namespace,
    # hash of the options and hash of the selector.
    #
    # @return [Integer] A hash value of the +Scope+ object.
    def hash
      [@collection.full_namespace, @opts.hash, @selector.hash].hash
    end

    # Iterate through documents returned by a query with this +Scope+.
    #
    # @return [Enumerator] The enumerator.
    #
    # @yieldparam doc [Hash] Each matching document.
    def each
      enum = cursor.to_enum
      enum.each do |doc|
        yield doc
      end if block_given?
      enum
    end

    private

    # Create a +Cursor+ using this +Scope+.
    #
    # @return [Cursor] The new +Cursor+ with this +Scope+.
    def cursor
      Cursor.new(self)
    end

    # Clone or dup the current +Scope+.
    #
    # The @opt and @selector instance variables are duped and the
    # +Collection+ reference remains intact.
    #
    # @param other [Scope] The +Scope+ to be cloned.
    #
    # @return [Scope] The new +Scope+.
    def initialize_copy(other)
      @collection = other.collection
      @opts = other.opts.dup
      @selector = other.selector.dup
    end

    # The read preference for this operation.
    #
    # @return [Symbol] This operation's read preference.
    def default_read(read = nil)
      @opts[:read] || @collection.read
    end

    # Extract query opts from @opts and return them in a separate hash.
    #
    # @return [Hash] The query options in their own hash.
    def query_opts_hash
      q_opts = @opts[:snapshot].nil? ? {} : { :snapshot => @opts[:snapshot] }
      q_opts[:max_scan] = @opts[:max_scan] unless @opts[:max_scan].nil?
      unless @opts[:show_disk_loc].nil?
        q_opts[:show_disk_loc] = @opts[:show_disk_loc]
      end
      q_opts
    end

    # Either return the option value or create a new +Scope+ with the option
    # value set.
    #
    # @return [Object, Scope] Either the option value or a new +Scope+.
    def set_option(field, value)
      return @opts[field] if value.nil?
      Scope.new(collection, selector, @opts.merge(field => value))
    end

    # Set the option value on this +Scope+.
    #
    # @return [Scope] self.
    def mutate(field, value)
      @opts.merge!(field => value) unless value.nil?
      self
    end

  end
end

# Copyright (C) 2013 10gen Inc.
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
  # A Scope can be modified using helpers.  Helpers can be chained, as each
  # one returns a Scope if arguments are provided.
  #
  # The query message is sent to the server when a "terminator" is called.
  # For example, when .each is called on a Scope, a Cursor object is created,
  # which then sends the query to the server.
  #
  # Scopes are not created directly by users.  Rather, Collection creates a
  # Scope when a CRUD operation is called and returns it to the user to interact
  # with.
  #
  # Note: The Scope API is semipublic.
  #
  # @api semipublic
  #
  class Scope

    attr_reader :collection, :selector, :opts

    # Creates a new Scope.
    #
    # @example Find all users named Emily.
    #   Scope.new(collection, {:name => 'Emily'})
    #
    # @example Find all users named Emily skipping 5 and returning 10.
    #   Scope.new(collection, {:name => 'Emily'}, :skip => 5, :limit => 10)
    #
    # @example Find all users using read preference :secondary_preferred.
    #   Scope.new(collection, {:name => 'Emily'}, :read => :secondary_preferred)
    #
    # @param collection [Collection] The collection to query.
    # @param selector [Hash] The query selector.
    # @param options [Hash] The additional query options.
    #
    # @option options :comment [String] Associate a comment with the query.
    # @option options :batch_size [Integer] The number of docs to return in responses.
    # @option options :fields [Hash] The fields to include or exclude in returned docs.
    # @option options :hint [Hash] Override default index selection.
    # @option options :limit [Integer] Max number of docs to return.
    # @option options :max_scan [Integer] The max number of docs to scan.
    # @option options :read [Symbol] The read preference to use for this query.
    # @option options :show_disk_loc [Boolean] Return disk location info with each doc.
    # @option options :skip [Integer] The number of documents to skip.
    # @option options :snapshot [Boolean] Prevents returning a doc more than once.
    # @option options :sort [Hash] The attributes/directions used to sort the results.
    #
    def initialize(collection, selector = {}, opts = {})
      @collection = collection
      @selector = selector.dup
      @opts = opts.dup
    end

    # Get a human-readable string representation of Scope.
    #
    # @return [String] a string representation of a Scope instance.
    #
    def inspect
      "<Mongo::Scope:0x#{self.object_id} namespace='#{@collection.db.name}." +
        "#{@collection.name}' @selector=#{@selector} @opts=#{@opts}>"
    end

    # Get the size of the result set for the query.
    #
    # @return [Integer] The number of documents in the result set.
    #
    def count
      @collection.count(Scope.new(@collection, @selector, @opts))
    end

    # Get the explain plan for the query.
    #
    # @return [Hash] a single document with the explain plan.
    #
    def explain
      explain_limit = limit || 0
      opts = @opts.merge({ :limit => -explain_limit.abs, explain: true })
      @collection.explain(Scope.new(@collection, @selector, opts))
    end

    # Get the distinct values for a specified field across a single collection.
    # Note that if a @selector is defined, it will be used in the anaylsis.
    #
    # @param key [Symbol, String] The field to collect distinct values from.
    #
    # @return [Hash] a doc with an array of the distinct values and query plan.
    #
    def distinct(key)
      @collection.distinct(self, key)
    end

    # Associate a comment with the query.
    # Set profilingLevel to 2 and the comment will be logged in the profile
    # collection along with the query.
    #
    # @param comment [String] The comment to be associated with this query.
    #
    # @return [String, Scope] either the comment or a new Scope.
    #
    def comment(comment=nil)
      return @opts[:comment] if comment.nil?
      Scope.new(collection, selector, @opts.merge(comment: comment))
    end

    # Modify this Scope to associate a comment with the query.
    #
    # @param comment [String] The comment to be associated with this query.
    #
    # @return [Scope] self.
    #
    def comment!(comment=nil)
      @opts.merge!(comment: comment) unless comment.nil?
      self
    end

    # The number of documents returned in each batch of results from MongoDB.
    # Specifying 1 or a negative number is analogous to setting a limit.
    #
    # @param batch_size [Integer] The size of each batch of results from MongoDB.
    #
    # @return [Integer, Scope] either the batch_size value or a new Scope.
    #
    def batch_size(batch_size=nil)
      return @opts[:batch_size] if batch_size.nil?
      Scope.new(collection, selector, @opts.merge(batch_size: batch_size))
    end

    # Modify this Scope to define the number of documents returned in each batch
    # of results from MongoDB.
    #
    # @param batch_size [Integer] The size of each batch of results from MongoDB.
    #
    # @return [Scope] self.
    #
    def batch_size!(batch_size=nil)
      @opts.merge!(batch_size: batch_size) unless batch_size.nil?
      self
    end

    # The fields to include or exclude from each doc in the result set.
    # A value of 0 excludes a field from the doc. A value of 1 includes it.
    # Values must all be 0 or all be 1, with the exception of the _id value.
    # The _id field is included by default. It must be excluded explicitly.
    #
    # @param fields [Hash] The field and 1 or 0, to include or exclude it.
    #
    # @return [Scope] either the fields or a new Scope.
    #
    def fields(fields=nil)
      return @opts[:fields] if fields.nil?
      Scope.new(collection, selector, @opts.merge(fields: fields))
    end

    # Modify this Scope to define the fields to include or exclude from each doc
    # in the result set.
    #
    # @param fields [Hash] The field and 1 or 0, to include or exclude it.
    #
    # @return [Scope] self.
    #
    def fields!(fields=nil)
      @opts.merge!(fields: fields) unless fields.nil?
      self
    end

    # The index that MongoDB will be forced to use for this query.
    #
    # @param hint [Hash] The index to use for the query.
    #
    # @return [Hash, Scope] either the hint or a new Scope.
    #
    def hint(hint=nil)
      return @opts[:hint] if hint.nil?
      Scope.new(collection, selector, @opts.merge(hint: hint))
    end

    # Modify this Scope to define the index that MongoDB will be forced
    # to use for this query.
    #
    # @param hint [Hash] The index to use for the query.
    #
    # @return [Scope] self.
    #
    def hint!(hint=nil)
      @opts.merge!(hint: hint) unless hint.nil?
      self
    end

    # The max number of docs to return from the query.
    #
    # @param limit [Integer] The number of docs to return.
    #
    # @return [Integer, Scope] either the limit or a new Scope.
    #
    def limit(limit=nil)
      return @opts[:limit] if limit.nil?
      Scope.new(collection, selector, @opts.merge(limit: limit))
    end

    # Modify this Scope to define the max number of docs to return from
    # the query.
    #
    # @param limit [Integer] The number of docs to return.
    #
    # @return [Scope] self.
    #
    def limit!(limit=nil)
      @opts.merge!(limit: limit) unless limit.nil?
      self
    end

    # The read preference to use for this query.
    # If none is specified for this query, the read preference of the collection
    # will be used.
    #
    # @param read [Symbol] The read preference to use for this query.
    #
    # @return [Symbol, Scope] either the read preference or a new Scope.
    #
    def read(read=nil)
      return default_read if read.nil?
      Scope.new(collection, selector, @opts.merge(read: read))
    end

    # Modify this Scope to define the read preference to use for this query.
    #
    # @param read [Symbol] The read preference to use for this query.
    #
    # @return [Scope] self.
    #
    def read!(read=nil)
      @opts.merge!(read: read) unless read.nil?
      self
    end

    # The number of docs to skip before returning results.
    #
    # @param skip [Integer] Number of docs to skip.
    #
    # @return [Integer, Scope] either the skip value or a new Scope.
    #
    def skip(skip=nil)
      return @opts[:skip] if skip.nil?
      Scope.new(collection, selector, @opts.merge(skip: skip))
    end

    # Modify this Scope to define the number of docs to skip before returning
    # results.
    #
    # @param skip [Integer] Number of docs to skip.
    #
    # @return [Scope] self.
    #
    def skip!(skip=nil)
      @opts.merge!(skip: skip) unless skip.nil?
      self
    end

    # The attributes by which the result set will be sorted.
    #
    # @param sort [Hash] The attributes and directions to sort by.
    #
    # @return [Hash, Scope] either the sort setting or a new Scope.
    #
    def sort(sort=nil)
      return @opts[:sort] if sort.nil?
      Scope.new(collection, selector, @opts.merge(sort: sort))
    end

    # Modify this Scope to define the attributes by which the result set
    # will be sorted.
    #
    # @param sort [Hash] The attributes and directions to sort by.
    #
    # @return [Scope] self.
    #
    def sort!(sort=nil)
      @opts.merge!(sort: sort) unless sort.nil?
      self
    end

    # Set options for this query.
    #
    # @param q_opts [Hash] Query options.
    #
    # @option q_opts :snapshot [Boolean] Prevents returning a doc more than once.
    # @option q_opts :max_scan [Integer] The max number of docs to scan.
    # @option q_opts :show_disk_loc [Boolean] Return disk location info with each doc.
    #
    # @return [Hash, Scope] either the q_opts or a new Scope
    #
    def query_opts(q_opts=nil)
      if q_opts.nil?
        return @opts.select do |k, v|
          [:snapshot, :max_scan, :show_disk_loc].include?(k)
        end
      end
      opts = @opts.dup
      [:snapshot, :max_scan, :show_disk_loc].each do |k|
        q_opts[k].nil? ? opts.delete(k) : opts.merge!(k => q_opts[k])
      end
      Scope.new(collection, selector, opts)
    end

    # Modify this Scope to set options for this query.
    #
    # @param q_opts [Hash] Query options.
    #
    # @option q_opts :snapshot [Boolean] Prevents returning a doc more than once.
    # @option q_opts :max_scan [Integer] The max number of docs to scan.
    # @option q_opts :show_disk_loc [Boolean] Return disk location info with each doc.
    #
    # @return [Scope] self
    #
    def query_opts!(q_opts=nil)
      return self if q_opts.nil?
      [:snapshot, :max_scan, :show_disk_loc].each do |k|
        q_opts[k].nil? ? @opts.delete(k) : opts.merge!(k => q_opts[k])
      end
      self
    end

    # Compare two Scope objects.
    #
    # @return [Boolean] equal if collection, selector, and opts of two Scopes match.
    #
    def ==(other)
      @collection == other.collection &&
        @selector == other.selector &&
        @opts == other.opts
    end
    alias :eql? :==

    def hash
      [@collection.full_namespace, @opts.hash, @selector.hash].hash
    end

    private

    def initialize_copy(other)
      @collection = other.collection
      @opts = other.opts.dup
      @selector = other.selector.dup
    end

    # The read preference for this operation.
    #
    # @return [Symbol] this operation's read preference.
    #
    def default_read(read=nil)
      @opts[:read] || @collection.read
    end

  end
end

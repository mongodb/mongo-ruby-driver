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
  # A Scope can be modified using helpers. Helpers can be chained, as each
  # one returns the Scope itself.
  #
  # The query message is sent to the server when a "terminator" is called.
  # For example, when .each is called on a Scope, a Cursor object is created,
  # which then sends the query to the server.
  #
  # Scopes are not created directly by users. Rather, Collection creates a
  # Scope when a CRUD operation is called and returns it to the user to interact
  # with.
  #
  # @api semipublic
  class Scope

    attr_reader :collection, :selector

    # Creates a new Scope
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
    # @option options :max_scan [Boolean] The max number of docs to scan.
    # @option options :read [Symbol] The read preference to use for this query.
    # @option options :return_key [Boolean] Only return the index field(s).
    # @option options :show_disk_loc [Boolean] Return disk location info with each doc.
    # @option options :skip [Integer] The number of documents to skip.
    # @option options :snapshot [Boolean] Prevents returning a doc more than once.
    # @option options :sort [Hash] The attributes/directions used to sort the results.
    #
    def initialize(collection, selector = {}, opts = {})
      @collection = collection
      @selector = selector
      @opts = opts
    end

    def inspect
      "<Mongo::Scope: namespace='#{@collection.db.name}.#{@collection.name}' " +
        "@selector=#{@selector} @opts=#{@opts}>"
    end

    # Get the size of the result set for the query.
    #
    # @return [Integer] The number of documents in the result set.
    def count
      @collection.count(Scope.new(@collection, @selector, @opts))
    end

    # Get the explain plan for the query.
    #
    # @return [Hash] a single document with the explain plan.
    def explain
      explain_limit = limit || 0
      opts = @opts.merge({ :limit => -explain_limit.abs, :explain => true })
      @collection.explain(Scope.new(@collection, @selector, opts))
    end

    # Associate a comment with the query.
    # Set profilingLevel to 2 and the comment will be logged in the profile
    # collection along with the query.
    #
    # @param comment [String] The comment to be associated with this query.
    # @return [String, Scope] either the comment or the Scope itself.
    def comment(comment=nil)
      return @opts[:comment] if comment.nil?
      @opts[:comment] = comment
      self
    end

    # The number of documents to return in each batch of results from MongoDB.
    # Specifying 1 or a negative number is analogous to setting a limit.
    #
    # @param batch_size [Integer] The size of each batch of results from MongoDB.
    # @return [Integer, Scope] either the batch_size value or the Scope itself.
    def batch_size(batch_size=nil)
      return @opts[:batch_size] if batch_size.nil?
      @opts[:batch_size] = batch_size
      self
    end

    # The fields to include or exclude from each doc in the result set.
    # A value of 0 excludes a field from the doc. A value of 1 includes it.
    # Values must all be 0 or all be 1, with the exception of the _id value.
    # The _id field is included by default. It must be excluded explicitly.
    #
    # @param fields [Hash] The field and 1 or 0, to include or exclude it.
    # @return [Hash, Scope] either the fields or the Scope itself.
    def fields(fields=nil)
      return @opts[:fields] if fields.nil?
      @opts[:fields] = fields
      self
    end

    # The index that MongoDB will be forced to use for this query.
    #
    # @param hint [Hash] The index to use for the query.
    # @return [Hash, Scope] either the hint or the Scope itself.
    def hint(hint=nil)
      return @opts[:hint] if hint.nil?
      @opts[:hint] = hint
      self
    end

    # The max number of docs to return from the query.
    #
    # @param limit [Integer] The number of docs to return.
    # @return [Integer, Scope] either the limit or the Scope itself.
    def limit(limit=nil)
      return @opts[:limit] if limit.nil?
      @opts[:limit] = limit
      self
    end

    # The max number of docs to scan when fulfilling the query.
    #
    # @param max_scan [Integer] The max number of docs to scan.
    # @return [Integer, Scope] either the max_scan value or the Scope itself.
    def max_scan(max_scan=nil)
      return @opts[:max_scan] if max_scan.nil?
      @opts[:max_scan] = max_scan
      self
    end

    # The read preference to use for this query.
    # If none is specified for this query, the read preference of the collection
    # will be used.
    #
    # @param read [Symbol] The read preference to use for this query.
    # @return [Symbol, Scope] either the read preference or the Scope itself.
    def read(read=nil)
      return default_read if read.nil?
      @opts[:read] = read
      self
    end

    # Only return the index field(s) for the results of this query.
    # If set to true and the query doesn't use the index, the docs will not have
    # any fields.
    #
    # @param return_key [Boolean] Only return the index field(s).
    # @return [Boolean, Scope] either the return_key setting or the Scope itself.
    def return_key(return_key=nil)
      return @opts[:return_key] if return_key.nil?
      @opts[:return_key] = return_key
      self
    end

    # Include a $diskLoc field in each result doc with disk location information.
    #
    # @param show_disk_loc [Boolean] Return disk location info with each doc.
    # @return [Boolean, Scope] either the show_disk_loc setting or the Scope itself.
    def show_disk_loc(show_disk_loc=nil)
      return @opts[:show_disk_loc] if show_disk_loc.nil?
      @opts[:show_disk_loc] = show_disk_loc
      self
    end

    # The number of docs to skip before returning results.
    #
    # @param skip [Integer] Number of docs to skip.
    # @return [Integer, Scope] either the skip value or the Scope itself.
    def skip(skip=nil)
      return @opts[:skip] if skip.nil?
      @opts[:skip] = skip
      self
    end

    # Prevents the server cursor from returning a document more than once.
    # Intervening write operations may result in a doc moving on disk and being
    # repeated in a result set.
    #
    # @param snapshot [Boolean] Prevent a doc from being returned more than once.
    # @return [Boolean, Scope] either the snapshot setting or the Scope itself.
    def snapshot(snapshot=nil)
      return @opts[:snapshot] if snapshot.nil?
      @opts[:snapshot] = snapshot
      self
    end

    # The attributes by which the result set will be sorted.
    #
    # @param sort [Hash] The attributes and directions to sort by.
    # @return [Boolean, Scope] either the sort setting or the Scope itself.
    def sort(sort=nil)
      return @opts[:sort] if sort.nil?
      @opts[:sort] = sort
      self
    end

    # Intersect the result set with another one.
    #
    # @param selector [Hash] A selector that will be merged with the existing one.
    # @return [Scope] the Scope itself.
    def intersect(selector = {})
      @selector.merge!(selector)
      self
    end

    private
    # The read preference for this operation.
    #
    # @return [Symbol] this operation's read preference.
    def default_read(read=nil)
      @opts[:read] || @collection.read
    end

  end
end

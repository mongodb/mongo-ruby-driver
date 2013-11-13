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

  # Client-side representation of an iterator over a query result set on
  # the server.
  #
  # A +Cursor+ is not created directly by a user. Rather, +Scope+ creates a
  # +Cursor+ in an Enumerable module method.
  #
  # @example Get an array of 5 users named Emily.
  #   users.find({:name => 'Emily'}).limit(5).to_a
  #
  # @example Call a block on each user doc.
  #   users.find.each { |doc| puts doc }
  #
  # @note The +Cursor+ API is semipublic.
  # @api semipublic
  class Cursor

    # Creates a +Cursor+ object.
    #
    # @param scope [Scope] The +Scope+ defining the query.
    def initialize(scope)
      @scope      = scope
      @cursor_id  = nil
      @collection = @scope.collection
      @client     = @collection.client
      @node       = nil
      @cache      = []
      @returned   = 0
    end

    # Get a human-readable string representation of +Cursor+.
    #
    # @return [String] A string representation of a +Cursor+ instance.
    def inspect
      "<Mongo::Cursor:0x#{object_id} @scope=#{@scope.inspect}>"
    end

    # Iterate through documents returned from the query.
    #
    # @yieldparam doc [Hash] Each matching document.
    def each
      yield fetch_doc until done?
    end

    private

    MAX_QUERY_TRIES = 3

    SPECIAL_FIELDS = [
      [:$query,          :selector],
      [:$readPreference, :read_pref],
      [:$orderby,        :sort],
      [:$hint,           :hint],
      [:$comment,        :comment],
      [:$snapshot,       :snapshot],
      [:$maxScan,        :max_scan],
      [:$showDiskLoc,    :show_disk_loc]
    ]

    # Whether we have iterated through all documents in the cache and retrieved
    # all results from the server.
    #
    # @return [true, false] If there are neither docs left in the cache
    #   or on the server for this query.
    def done?
      @cache.empty? && exhausted?
    end

    # Get the next doc in the result set.
    #
    # If the cache is empty, request more docs from the server.
    #
    # Check if the doc is an error doc before returning.
    #
    # @return [Hash] The next doc in the result set.
    def fetch_doc
      request_docs if @cache.empty?
      doc = @cache.shift
      doc unless error?(doc)
    end

    # Close the cursor on the server.
    #
    # If there is neither a node set or if the cursor is already closed,
    # return nil. Otherwise, send a kill cursor command.
    def close
      return nil if @node.nil? || closed?
      kill_cursors
    end

    # Request documents from the server.
    #
    # Close the cursor on the server if all docs have been retreived.
    def request_docs
      if !query_run?
        send_initial_query
      else
        send_get_more
      end
      close if exhausted?
    end

    # Send a message to a node and collect the results.
    #
    # @todo: Brandon: verify connecton interface
    def send_and_receive(connection, message)
      results, @node = connection.send_and_receive(MAX_QUERY_TRIES, message)
      @cursor_id     = results[:cursor_id]
      @returned      += results[:nreturned]
      @cache         += results[:docs]
    end

    # Build the query selector and initial +Query+ message.
    #
    # @return [Query] The +Query+ message.
    def initial_query_message
      selector = has_special_fields? ? special_selector : selector
      Mongo::Protocol::Query.new(db_name, coll_name, selector, query_opts)
    end

    # Send the initial query message to a node.
    #
    # @todo: Brandon: verify client interface
    def send_initial_query
      @client.with_node(read) do |connection|
        send_and_receive(connection, initial_query_message)
      end
    end

    # Build the +GetMore+ message using the cursor id and number of documents
    # to return.
    #
    # @return [GetMore] The +GetMore+ message
    def get_more_message
      Mongo::Protocol::GetMore.new(db_name, coll_name, to_return, @cursor_id)
    end

    # Send a +GetMore+ message to a node to get another batch of results.
    #
    # @todo: define exceptions
    def send_get_more
      raise Exception, 'No node set' unless @node
      @node.with_connection do |connection|
        send_and_receive(connection, get_more_message)
      end
    end

    # Build a +KillCursors+ message using this cursor's id.
    #
    # @return [KillCursors] The +KillCursors+ message.
    def kill_cursors_message
      Mongo::Protocol::KillCursors.new([@cursor_id])
    end

    # Send a +KillCursors+ message to the server and set the cursor id to 0.
    #
    # @todo: Brandon: verify node interface
    def kill_cursors
      @node.with_connection do |connection|
        connection.send_message(kill_cursors_message)
      end
      @cursor_id = 0
    end

    # Determine whether this query has special fields.
    #
    # @return [true, false] Whether the query has special fields.
    def has_special_fields?
      !!(opts || sort || hint || comment || read_pref)
    end

    # Get the read preference for this query.
    #
    # @return [Hash, nil] The read preference or nil.
    def read_pref
      @client.mongos? ? read.mongos : read
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
    def query_opts
      {
        :fields => @scope.fields,
        :skip => @scope.skip,
        :limit => to_return,
        :flags => flags,
      }
    end

    # The query options set on the +Scope+.
    #
    # @return [Hash] The query options set on the +Scope+.
    def opts
      @scope.query_opts.empty? ? nil : @scope.query_opts
    end

    # The flags set on this query.
    #
    # @return [Array] List of flags to be set on the query message.
    # @todo: add no_cursor_timeout option
    def flags
      flags << :slave_ok if need_slave_ok?
    end

    # Check whether the document returned is an error document.
    #
    # @return [true, false] Whether the document is an error document.
    # @todo: Emily: do this.
    def error?(doc)
      false
    end

    # Delta between the number of documents retrieved and the documents
    # requested.
    #
    # @return [Integer] Delta between the number of documents retrieved
    #   and the documents requested.
    def remaining_limit
      limit - @returned
    end

    # The number of documents to return in each batch from the server.
    #
    # @return [Integer] The number of documents to return in each batch from
    #   the server.
    def batch_size
      @scope.batch_size && @scope.batch_size > 0 ? @scope.batch_size : limit
    end

    # Whether a limit should be specified.
    #
    # @return [true, false] Whether a limit should be specified.
    def use_limit?
      limited? && batch_size >= remaining_limit
    end

    # The number of documents to return in the next batch.
    #
    # @return [Integer] The number of documents to return in the next batch.
    def to_return
      use_limit? ? remaining_limit : batch_size
    end

    # Whether this query has a limit.
    #
    # @return [true, false] Whether this query has a limit.
    def limited?
      limit > 0 if limit
    end

    # Whether the cursor has been closed on the server.
    #
    # @return [true, false] Whether the cursor has been closed on the server.
    def closed?
      @cursor_id == 0
    end

    # The read preference to use for this query.
    #
    # @return [Hash] The read preference to use for this query.
    def read
      @scope.read
    end

    # The name of the database.
    #
    # @return [String] The name of the database.
    def db_name
      @collection.database.name
    end

    # The name of the collection.
    #
    # @return [String] The name of the collection.
    def coll_name
      @collection.name
    end

    # Whether the initial query message has already been sent.
    #
    # @return [true, false] Whether the query has already been sent to
    #   the server.
    def query_run?
      !@node.nil?
    end

    # Whether all query results have been retrieved from the server.
    #
    # @return [true, false] Whether all results have been retrieved from
    #   the server.
    def exhausted?
      limited? ? (@returned >= limit) : closed?
    end

    # Whether the slave ok bit needs to be set on the wire protocol message.
    #
    # @return [true, false] Whether the slave ok bit needs to be set.
    def need_slave_ok?
      !primary?
    end

    # Whether the read preference mode is primary.
    #
    # @return [true, false] Whether the read preference mode is primary.
    def primary?
      read.primary?
    end

    # The selector used for the query.
    #
    # @return [Hash] The selector for the query.
    def selector
      @scope.selector
    end

    # The max scan option set on the +Scope+.
    #
    # @return [Integer, nil] The max scan setting on the +Scope+ or nil.
    def max_scan
      @scope.query_opts[:max_scan]
    end

    # The snapshot setting on the +Scope+.
    #
    # @return [true, false, nil] The snapshot setting on +Scope+ or nil.
    def snapshot
      @scope.query_opts[:snapshot]
    end

    # The show disk location setting on the +Scope+.
    #
    # @return [true, false, nil] Either the show disk location setting on
    #   +Scope+ or nil.
    def show_disk_loc
      @scope.query_opts[:show_disk_loc]
    end

    # The sort setting on the +Scope+.
    #
    # @return [Hash, nil] Either the sort setting on +Scope+ or nil.
    def sort
      @scope.sort
    end

    # The hint setting on the +Scope+.
    #
    # @return [Hash, nil] Either the hint setting on +Scope+ or nil.
    def hint
      @scope.hint
    end

    # The comment setting on the +Scope+.
    #
    # @return [String, nil] Either the comment setting on +Scope+ or nil.
    def comment
      @scope.comment
    end

    # The limit setting on the +Scope+.
    #
    # @return [Integer, nil] Either the limit setting on +Scope+ or nil.
    def limit
      @scope.limit
    end

  end
end

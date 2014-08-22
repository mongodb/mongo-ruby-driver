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

  # Client-side representation of an iterator over a query result set on
  # the server.
  #
  # A +Cursor+ is not created directly by a user. Rather, +CollectionView+
  # creates a +Cursor+ in an Enumerable module method.
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
    # @param view [ CollectionView ] The +CollectionView+ defining the query.
    def initialize(view, response, server)
      @view       = view
      @collection = @view.collection
      @client     = @collection.client
      @server     = server
      process_response(response)
    end

    # Get a human-readable string representation of +Cursor+.
    #
    # @return [String] A string representation of a +Cursor+ instance.
    def inspect
      "<Mongo::Cursor:0x#{object_id} @view=#{@view.inspect}>"
    end

    # Iterate through documents returned from the query.
    #
    # @yieldparam [Hash] Each matching document.
    def each
      yield fetch_doc until done?
    end

    private

    # Process the response returned from the server either from
    # the initial query or from the get more operation.
    #
    # @params [ Object ] The response from the operation.
    def process_response(response)
      @cache     = (@cache || []) + response.documents
      @returned  = (@returned || 0) + @cache.length
      @cursor_id = response.cursor_id
    end

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
    # If there is neither a server set or if the cursor is already closed,
    # return nil. Otherwise, send a kill cursor command.
    def close
      return nil if @server.nil? || closed?
      kill_cursors
    end

    # Request documents from the server.
    #
    # Close the cursor on the server if all docs have been retreived.
    def request_docs
      send_get_more
      close if exhausted?
    end

    # Build the +GetMore+ message using the cursor id and number of documents
    # to return.
    #
    # @return [Hash] The +GetMore+ operation spec.
    def get_more_spec
      { :to_return => to_return,
        :cursor_id => @cursor_id,
        :db_name   => @collection.database.name,
        :coll_name => @collection.name }
    end

    def get_more_op
      Mongo::Operation::Read::GetMore.new(get_more_spec)
    end

    # Send a +GetMore+ message to a server to get another batch of results.
    #
    # @todo: define exceptions
    def send_get_more
      raise Exception, 'No server set' unless @server
      response = get_more_op.execute(@server.context)
      process_response(response)
    end

    # Build a +KillCursors+ message using this cursor's id.
    #
    # @return [KillCursors] The +KillCursors+ message.
    def kill_cursors_op
      Mongo::Operation::KillCursors.new({ :cursor_ids => [@cursor_id] })
    end

    # Send a +KillCursors+ message to the server and set the cursor id to 0.
    def kill_cursors
      kill_cursors_op.execute(@server.context)
      @cursor_id = 0
    end

    # Check whether the document returned is an error document.
    #
    # @return [true, false] Whether the document is an error document.
    # @todo: method on response?
    def error?(doc)
      # @todo implement this
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
      @view.batch_size && @view.batch_size > 0 ? @view.batch_size : limit
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

    # Whether all query results have been retrieved from the server.
    #
    # @return [true, false] Whether all results have been retrieved from
    #   the server.
    def exhausted?
      return true if closed?
      limited? && (@returned >= limit)
    end

    # The limit setting on the +CollectionView+.
    #
    # @return [Integer, nil] Either the limit setting on +CollectionView+ or nil.
    def limit
      @view.limit
    end
  end
end


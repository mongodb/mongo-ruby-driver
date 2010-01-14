# Copyright (C) 2008-2009 10gen Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

module Mongo

  # A cursor over query results. Returned objects are hashes.
  class Cursor
    include Mongo::Conversions
    include Enumerable

    attr_reader :collection, :selector, :admin, :fields,
      :order, :hint, :snapshot, :timeout,
      :full_collection_name

    # Create a new cursor.
    #
    # Note: cursors are created when executing queries using [Collection#find] and other
    # similar methods. Application developers shouldn't have to create cursors manually.
    #
    # @return [Cursor]
    def initialize(collection, options={})
      @db         = collection.db
      @collection = collection
      @connection = @db.connection

      @selector   = convert_selector_for_query(options[:selector])
      @fields     = convert_fields_for_query(options[:fields])
      @admin      = options[:admin]    || false
      @skip       = options[:skip]     || 0
      @limit      = options[:limit]    || 0
      @order      = options[:order]
      @hint       = options[:hint]
      @snapshot   = options[:snapshot]
      @timeout    = options[:timeout]  || false
      @explain    = options[:explain]
      @socket     = options[:socket]

      @full_collection_name = "#{@collection.db.name}.#{@collection.name}"
      @cache = []
      @closed = false
      @query_run = false
    end

    # Get the next document specified the cursor options.
    #
    # @return [Hash, Nil] the next document or Nil if no documents remain.
    def next_document
      refill_via_get_more if num_remaining == 0
      doc = @cache.shift

      if doc && doc['$err']
        err = doc['$err']

        # If the server has stopped being the master (e.g., it's one of a
        # pair but it has died or something like that) then we close that
        # connection. The next request will re-open on master server.
        if err == "not master"
          raise ConnectionFailure, err
          @connection.close
        end

        raise OperationFailure, err
      end

      doc
    end

    # @deprecated use Cursor#next_document instead.
    def next_object
      warn "Cursor#next_object is deprecated; please use Cursor#next_document instead."
      next_document
    end

    # Get the size of the result set for this query.
    #
    # @return [Integer] the number of objects in the result set for this query. Does
    #   not take limit and skip into account. 
    #
    # @raise [OperationFailure] on a database error.
    def count
      command = OrderedHash["count",  @collection.name,
                            "query",  @selector,
                            "fields", @fields]
      response = @db.command(command)
      return response['n'].to_i if response['ok'] == 1
      return 0 if response['errmsg'] == "ns missing"
      raise OperationFailure, "Count failed: #{response['errmsg']}"
    end

    # Sort this cursor's results.
    #
    # This method overrides any sort order specified in the Collection#find
    # method, and only the last sort applied has an effect.
    #
    # @param [Symbol, Array] key_or_list either 1) a key to sort by or 2) 
    #   an array of [key, direction] pairs to sort by. Direction should
    #   be specified as Mongo::ASCENDING (or :ascending / :asc) or Mongo::DESCENDING (or :descending / :desc)
    #
    # @raise [InvalidOperation] if this cursor has already been used.
    #
    # @raise [InvalidSortValueError] if the specified order is invalid.
    def sort(key_or_list, direction=nil)
      check_modifiable

      if !direction.nil?
        order = [[key_or_list, direction]]
      else
        order = key_or_list
      end

      @order = order
      self
    end

    # Limit the number of results to be returned by this cursor.
    #
    # This method overrides any limit specified in the Collection#find method,
    # and only the last limit applied has an effect.
    #
    # @return [Integer] the current number_to_return if no parameter is given.
    #
    # @raise [InvalidOperation] if this cursor has already been used.
    def limit(number_to_return=nil)
      return @limit unless number_to_return
      check_modifiable
      raise ArgumentError, "limit requires an integer" unless number_to_return.is_a? Integer

      @limit = number_to_return
      self
    end

    # Skips the first +number_to_skip+ results of this cursor.
    # Returns the current number_to_skip if no parameter is given.
    #
    # This method overrides any skip specified in the Collection#find method,
    # and only the last skip applied has an effect.
    #
    # @return [Integer]
    #
    # @raise [InvalidOperation] if this cursor has already been used.
    def skip(number_to_skip=nil)
      return @skip unless number_to_skip
      check_modifiable
      raise ArgumentError, "skip requires an integer" unless number_to_skip.is_a? Integer

      @skip = number_to_skip
      self
    end

    # Iterate over each document in this cursor, yielding it to the given
    # block.
    #
    # Iterating over an entire cursor will close it.
    #
    # @yield passes each document to a block for processing.
    #
    # @example if 'comments' represents a collection of comments:
    #   comments.find.each do |doc|
    #     puts doc['user']
    #   end
    def each
      num_returned = 0
      while more? && (@limit <= 0 || num_returned < @limit)
        yield next_document
        num_returned += 1
      end
    end

    # Receive all the documents from this cursor as an array of hashes.
    #
    # Note: use of this method is discouraged - in most cases, it's much more
    # efficient to retrieve documents as you need them by iterating over the cursor.
    #
    # @return [Array] an array of documents.
    #
    # @raise [InvalidOperation] if this cursor has already been used or if
    #   this method has already been called on the cursor.
    def to_a
      raise InvalidOperation, "can't call Cursor#to_a on a used cursor" if @query_run
      rows = []
      num_returned = 0
      while more? && (@limit <= 0 || num_returned < @limit)
        rows << next_document
        num_returned += 1
      end
      rows
    end

    # Get the explain plan for this cursor.
    #
    # @return [Hash] a document containing the explain plan for this cursor.
    def explain
      c = Cursor.new(@collection, query_options_hash.merge(:limit => -@limit.abs, :explain => true))
      explanation = c.next_document
      c.close

      explanation
    end

    # Close the cursor.
    #
    # Note: if a cursor is read until exhausted (read until Mongo::Constants::OP_QUERY or
    # Mongo::Constants::OP_GETMORE returns zero for the cursor id), there is no need to
    # close it manually.
    #
    # Note also: Collection#find takes an optional block argument which can be used to
    # ensure that your cursors get closed.
    #
    # @return [True]
    def close
      if @cursor_id
        message = ByteBuffer.new([0, 0, 0, 0])
        message.put_int(1)
        message.put_long(@cursor_id)
        @connection.send_message(Mongo::Constants::OP_KILL_CURSORS, message, "cursor.close()")
      end
      @cursor_id = 0
      @closed    = true
    end

    # Is this cursor closed?
    #
    # @return [Boolean]
    def closed?; @closed; end

    # Returns an integer indicating which query options have been selected.
    #
    # @return [Integer]
    #
    # @see http://www.mongodb.org/display/DOCS/Mongo+Wire+Protocol#MongoWireProtocol-Mongo::Constants::OPQUERY
    # The MongoDB wire protocol.
    def query_opts
      timeout  = @timeout ? 0 : Mongo::Constants::OP_QUERY_NO_CURSOR_TIMEOUT
      slave_ok = @connection.slave_ok? ? Mongo::Constants::OP_QUERY_SLAVE_OK : 0
      slave_ok + timeout
    end

    # Get the query options for this Cursor.
    #
    # @return [Hash]
    def query_options_hash
      { :selector => @selector,
        :fields   => @fields,
        :admin    => @admin,
        :skip     => @skip_num,
        :limit    => @limit_num,
        :order    => @order,
        :hint     => @hint,
        :snapshot => @snapshot,
        :timeout  => @timeout }
    end

    private

    # Convert the +:fields+ parameter from a single field name or an array
    # of fields names to a hash, with the field names for keys and '1' for each
    # value.
    def convert_fields_for_query(fields)
      case fields
        when String, Symbol
          {fields => 1}
        when Array
          return nil if fields.length.zero?
          returning({}) do |hash|
            fields.each { |field| hash[field] = 1 }
          end
      end
    end

    # Set the query selector hash. If the selector is a Code or String object,
    # the selector will be used in a $where clause.
    # See http://www.mongodb.org/display/DOCS/Server-side+Code+Execution
    def convert_selector_for_query(selector)
      case selector
        when Hash
         selector
        when nil
          {}
        when String
          {"$where" => Code.new(selector)}
        when Code
          {"$where" => selector}
      end
    end

    # Returns true if the query contains order, explain, hint, or snapshot.
    def query_contains_special_fields?
      @order || @explain || @hint || @snapshot
    end

    # Return a number of documents remaining for this cursor.
    def num_remaining
      refill_via_get_more if @cache.length == 0
      @cache.length
    end

    # Internal method, not for general use. Return +true+ if there are
    # more records to retrieve. This method does not check @limit;
    # Cursor#each is responsible for doing that.
    def more?
      num_remaining > 0
    end

    def refill_via_get_more
      return if send_initial_query || @cursor_id.zero?
      message = ByteBuffer.new([0, 0, 0, 0])

      # DB name.
      db_name = @admin ? 'admin' : @db.name
      BSON_RUBY.serialize_cstr(message, "#{db_name}.#{@collection.name}")

      # Number of results to return; db decides for now.
      message.put_int(0)

      # Cursor id.
      message.put_long(@cursor_id)
      results, @n_received, @cursor_id = @connection.receive_message(Mongo::Constants::OP_GET_MORE, message, "cursor.get_more()", @socket)
      @cache += results
      close_cursor_if_query_complete
    end

    # Run query the first time we request an object from the wire
    def send_initial_query
      if @query_run
        false
      else
        message = construct_query_message
        results, @n_received, @cursor_id = @connection.receive_message(Mongo::Constants::OP_QUERY, message,
            (query_log_message if @connection.logger), @socket)
        @cache += results
        @query_run = true
        close_cursor_if_query_complete
        true
      end
    end

    def construct_query_message
      message = ByteBuffer.new
      message.put_int(query_opts)
      db_name = @admin ? 'admin' : @db.name
      BSON_RUBY.serialize_cstr(message, "#{db_name}.#{@collection.name}")
      message.put_int(@skip)
      message.put_int(@limit)
      selector = @selector
      if query_contains_special_fields?
        selector = selector_with_special_query_fields
      end
      message.put_array(BSON.serialize(selector, false).to_a)
      message.put_array(BSON.serialize(@fields, false).to_a) if @fields
      message
    end

    def query_log_message
      "#{@admin ? 'admin' : @db.name}.#{@collection.name}.find(#{@selector.inspect}, #{@fields ? @fields.inspect : '{}'})" +
      "#{@skip != 0 ? ('.skip(' + @skip.to_s + ')') : ''}#{@limit != 0 ? ('.limit(' + @limit.to_s + ')') : ''}"
    end

    def selector_with_special_query_fields
      sel = OrderedHash.new
      sel['query']     = @selector
      sel['orderby']   = formatted_order_clause if @order
      sel['$hint']     = @hint if @hint && @hint.length > 0
      sel['$explain']  = true if @explain
      sel['$snapshot'] = true if @snapshot
      sel
    end

    def formatted_order_clause
      case @order
        when String, Symbol then string_as_sort_parameters(@order)
        when Array then array_as_sort_parameters(@order)
        else
          raise InvalidSortValueError, "Illegal sort clause, '#{@order.class.name}'; must be of the form " +
            "[['field1', '(ascending|descending)'], ['field2', '(ascending|descending)']]"
      end
    end

    def to_s
      "DBResponse(flags=#@result_flags, cursor_id=#@cursor_id, start=#@starting_from)"
    end

    def close_cursor_if_query_complete
      close if @limit > 0 && @n_received >= @limit
    end

    def check_modifiable
      if @query_run || @closed
        raise InvalidOperation, "Cannot modify the query once it has been run or closed."
      end
    end
  end
end

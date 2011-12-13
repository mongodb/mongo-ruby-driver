# encoding: UTF-8

# Copyright (C) 2008-2011 10gen Inc.
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
    include Enumerable
    include Mongo::Constants
    include Mongo::Conversions
    include Mongo::Logging

    attr_reader :collection, :selector, :fields,
      :order, :hint, :snapshot, :timeout,
      :full_collection_name, :transformer,
      :options, :cursor_id, :show_disk_loc

    # Create a new cursor.
    #
    # Note: cursors are created when executing queries using [Collection#find] and other
    # similar methods. Application developers shouldn't have to create cursors manually.
    #
    # @return [Cursor]
    #
    # @core cursors constructor_details
    def initialize(collection, opts={})
      @cursor_id  = nil

      @db         = collection.db
      @collection = collection
      @connection = @db.connection
      @logger     = @connection.logger

      # Query selector
      @selector   = opts[:selector] || {}

      # Special operators that form part of $query
      @order      = opts[:order]
      @explain    = opts[:explain]
      @hint       = opts[:hint]
      @snapshot   = opts[:snapshot]
      @max_scan   = opts.fetch(:max_scan, nil)
      @return_key = opts.fetch(:return_key, nil)
      @show_disk_loc = opts.fetch(:show_disk_loc, nil)

      # Wire-protocol settings
      @fields     = convert_fields_for_query(opts[:fields])
      @skip       = opts[:skip]     || 0
      @limit      = opts[:limit]    || 0
      @tailable   = opts[:tailable] || false
      @timeout    = opts.fetch(:timeout, true)
      @options    = 0

      # Use this socket for the query
      @socket     = opts[:socket]

      @closed       = false
      @query_run    = false

      @transformer = opts[:transformer]
      if value = opts[:read]
        Mongo::Support.validate_read_preference(value)
      else
        value = collection.read_preference
      end
      @read_preference = value.is_a?(Hash) ? value.dup : value
      batch_size(opts[:batch_size] || 0)

      @full_collection_name = "#{@collection.db.name}.#{@collection.name}"
      @cache        = []
      @returned     = 0

      if(!@timeout)
        add_option(OP_QUERY_NO_CURSOR_TIMEOUT)
      end
      if(@read_preference != :primary)
        add_option(OP_QUERY_SLAVE_OK)
      end
      if(@tailable)
        add_option(OP_QUERY_TAILABLE)
      end

      if @collection.name =~ /^\$cmd/ || @collection.name =~ /^system/
        @command = true
      else
        @command = false
      end

      @checkin_read_pool = false
      @checkin_connection = false
      @read_pool = nil
    end

    # Guess whether the cursor is alive on the server.
    #
    # Note that this method only checks whether we have
    # a cursor id. The cursor may still have timed out
    # on the server. This will be indicated in the next
    # call to Cursor#next.
    #
    # @return [Boolean]
    def alive?
      @cursor_id && @cursor_id != 0
    end

    # Get the next document specified the cursor options.
    #
    # @return [Hash, Nil] the next document or Nil if no documents remain.
    def next
      if @cache.length == 0
        if @query_run && (@options & OP_QUERY_EXHAUST != 0)
          close
          return nil
        else
          refresh
        end
      end
      doc = @cache.shift

      if doc && doc['$err']
        err = doc['$err']

        # If the server has stopped being the master (e.g., it's one of a
        # pair but it has died or something like that) then we close that
        # connection. The next request will re-open on master server.
        if err.include?("not master")
          @connection.close
          raise ConnectionFailure.new(err, doc['code'], doc)
        end

        raise OperationFailure.new(err, doc['code'], doc)
      end

      if @transformer.nil?
        doc
      else
        @transformer.call(doc) if doc
      end
    end
    alias :next_document :next

    # Reset this cursor on the server. Cursor options, such as the
    # query string and the values for skip and limit, are preserved.
    def rewind!
      close
      @cache.clear
      @cursor_id  = nil
      @closed     = false
      @query_run  = false
      @n_received = nil
      true
    end

    # Determine whether this cursor has any remaining results.
    #
    # @return [Boolean]
    def has_next?
      num_remaining > 0
    end

    # Get the size of the result set for this query.
    #
    # @param [Boolean] whether of not to take notice of skip and limit
    #
    # @return [Integer] the number of objects in the result set for this query.
    #
    # @raise [OperationFailure] on a database error.
    def count(skip_and_limit = false)
      command = BSON::OrderedHash["count",  @collection.name, "query",  @selector]

      if skip_and_limit
        command.merge!(BSON::OrderedHash["limit", @limit]) if @limit != 0
        command.merge!(BSON::OrderedHash["skip", @skip]) if @skip != 0
      end

      command.merge!(BSON::OrderedHash["fields", @fields])

      response = @db.command(command)
      return response['n'].to_i if Mongo::Support.ok?(response)
      return 0 if response['errmsg'] == "ns missing"
      raise OperationFailure.new("Count failed: #{response['errmsg']}", response['code'], response)
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
    #
    # @core limit limit-instance_method
    def limit(number_to_return=nil)
      return @limit unless number_to_return
      check_modifiable

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

      @skip = number_to_skip
      self
    end

    # Set the batch size for server responses.
    #
    # Note that the batch size will take effect only on queries
    # where the number to be returned is greater than 100.
    #
    # @param [Integer] size either 0 or some integer greater than 1. If 0,
    #   the server will determine the batch size.
    #
    # @return [Cursor]
    def batch_size(size=nil)
      return @batch_size unless size
      check_modifiable
      if size < 0 || size == 1
        raise ArgumentError, "Invalid value for batch_size #{size}; must be 0 or > 1."
      else
        @batch_size = @limit != 0 && size > @limit ? @limit : size
      end

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
      while doc = self.next
        yield doc
      end
    end

    # Receive all the documents from this cursor as an array of hashes.
    #
    # Notes:
    #
    # If you've already started iterating over the cursor, the array returned
    # by this method contains only the remaining documents. See Cursor#rewind! if you
    # need to reset the cursor.
    #
    # Use of this method is discouraged - in most cases, it's much more
    # efficient to retrieve documents as you need them by iterating over the cursor.
    #
    # @return [Array] an array of documents.
    def to_a
      super
    end

    # Get the explain plan for this cursor.
    #
    # @return [Hash] a document containing the explain plan for this cursor.
    #
    # @core explain explain-instance_method
    def explain
      c = Cursor.new(@collection,
        query_options_hash.merge(:limit => -@limit.abs, :explain => true))
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
      if @cursor_id && @cursor_id != 0
        message = BSON::ByteBuffer.new([0, 0, 0, 0])
        message.put_int(1)
        message.put_long(@cursor_id)
        log(:debug, "Cursor#close #{@cursor_id}")
        @connection.send_message(Mongo::Constants::OP_KILL_CURSORS, message, :connection => :reader)
      end
      @cursor_id = 0
      @closed    = true
    end

    # Is this cursor closed?
    #
    # @return [Boolean]
    def closed?
      @closed
    end

    # Returns an integer indicating which query options have been selected.
    #
    # @return [Integer]
    #
    # @see http://www.mongodb.org/display/DOCS/Mongo+Wire+Protocol#MongoWireProtocol-Mongo::Constants::OPQUERY
    # The MongoDB wire protocol.
    def query_opts
      warn "The method Cursor#query_opts has been deprecated " +
        "and will removed in v2.0. Use Cursor#options instead."
      @options
    end

    # Add an option to the query options bitfield.
    #
    # @param opt a valid query option
    #
    # @raise InvalidOperation if this method is run after the cursor has bee
    #   iterated for the first time.
    #
    # @return [Integer] the current value of the options bitfield for this cursor.
    #
    # @see http://www.mongodb.org/display/DOCS/Mongo+Wire+Protocol#MongoWireProtocol-Mongo::Constants::OPQUERY
    def add_option(opt)
      check_modifiable

      @options |= opt
      @options
    end

    # Remove an option from the query options bitfield.
    #
    # @param opt a valid query option
    #
    # @raise InvalidOperation if this method is run after the cursor has bee
    #   iterated for the first time.
    #
    # @return [Integer] the current value of the options bitfield for this cursor.
    #
    # @see http://www.mongodb.org/display/DOCS/Mongo+Wire+Protocol#MongoWireProtocol-Mongo::Constants::OPQUERY
    def remove_option(opt)
      check_modifiable

      @options &= ~opt
      @options
    end

    # Get the query options for this Cursor.
    #
    # @return [Hash]
    def query_options_hash
      { :selector => @selector,
        :fields   => @fields,
        :skip     => @skip,
        :limit    => @limit,
        :order    => @order,
        :hint     => @hint,
        :snapshot => @snapshot,
        :timeout  => @timeout,
        :max_scan => @max_scan,
        :return_key => @return_key,
        :show_disk_loc => @show_disk_loc }
    end

    # Clean output for inspect.
    def inspect
      "<Mongo::Cursor:0x#{object_id.to_s(16)} namespace='#{@db.name}.#{@collection.name}' " +
        "@selector=#{@selector.inspect} @cursor_id=#{@cursor_id}>"
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
          fields.each_with_object({}) { |field, hash| hash[field] = 1 }
        when Hash
          return fields
      end
    end

    # Return the number of documents remaining for this cursor.
    def num_remaining
      if @cache.length == 0
        if @query_run && (@options & OP_QUERY_EXHAUST != 0)
          close
          return 0
        else
          refresh
        end
      end

      @cache.length
    end

    # Refresh the documents in @cache. This means either
    # sending the initial query or sending a GET_MORE operation.
    def refresh
      if !@query_run
        send_initial_query
      elsif !@cursor_id.zero?
        send_get_more
      end
    end

    def send_initial_query
      message = construct_query_message
      sock    = @socket || checkout_socket_from_connection
      instrument(:find, instrument_payload) do
        begin
        results, @n_received, @cursor_id = @connection.receive_message(
          Mongo::Constants::OP_QUERY, message, nil, sock, @command,
          nil, @options & OP_QUERY_EXHAUST != 0)
        rescue ConnectionFailure, OperationFailure, OperationTimeout => ex
          force_checkin_socket(sock)
          raise ex
        end
        checkin_socket(sock) unless @socket
        @returned += @n_received
        @cache += results
        @query_run = true
        close_cursor_if_query_complete
      end
    end

    def send_get_more
      message = BSON::ByteBuffer.new([0, 0, 0, 0])

      # DB name.
      BSON::BSON_RUBY.serialize_cstr(message, "#{@db.name}.#{@collection.name}")

      # Number of results to return.
      if @limit > 0
        limit = @limit - @returned
        if @batch_size > 0
          limit = limit < @batch_size ? limit : @batch_size
        end
        message.put_int(limit)
      else
        message.put_int(@batch_size)
      end

      # Cursor id.
      message.put_long(@cursor_id)
      log(:debug, "cursor.refresh() for cursor #{@cursor_id}") if @logger
      sock = @socket || checkout_socket_for_op_get_more

      begin
      results, @n_received, @cursor_id = @connection.receive_message(
        Mongo::Constants::OP_GET_MORE, message, nil, sock, @command, nil)
      rescue ConnectionFailure, OperationFailure, OperationTimeout => ex
        force_checkin_socket(sock)
        raise ex
      end
      checkin_socket(sock) unless @socket
      @returned += @n_received
      @cache += results
      close_cursor_if_query_complete
    end

    def checkout_socket_from_connection
      socket = nil
      begin
        @checkin_connection = true
        if @command || @read_preference == :primary
          socket = @connection.checkout_writer
        else
          @read_pool = @connection.read_pool
          socket = @connection.checkout_reader
        end
      rescue SystemStackError, NoMemoryError, SystemCallError => ex
        @connection.close
        raise ex
      end

      socket
    end

    def checkout_socket_for_op_get_more
      if @read_pool && (@read_pool != @connection.read_pool)
        checkout_socket_from_read_pool
      else
        checkout_socket_from_connection
      end
    end

    def checkout_socket_from_read_pool
      new_pool = @connection.secondary_pools.detect do |pool|
        pool.host == @read_pool.host && pool.port == @read_pool.port
      end
      if new_pool
        sock = nil
        begin
          @read_pool = new_pool
          sock = new_pool.checkout
          @checkin_read_pool = true
        rescue SystemStackError, NoMemoryError, SystemCallError => ex
          @connection.close
          raise ex
        end
        return sock
      else
        raise Mongo::OperationFailure, "Failure to continue iterating " +
          "cursor because the the replica set member persisting this " +
          "cursor at #{@read_pool.host_string} cannot be found."
      end
    end

    def checkin_socket(sock)
      if @checkin_read_pool
        @read_pool.checkin(sock)
        @checkin_read_pool = false
      elsif @checkin_connection
        if @command || @read_preference == :primary
          @connection.checkin_writer(sock)
        else
          @connection.checkin_reader(sock)
        end
        @checkin_connection = false
      end
    end

    def force_checkin_socket(sock)
      if @checkin_read_pool
        @read_pool.checkin(sock)
        @checkin_read_pool = false
      else
        if @command || @read_preference == :primary
          @connection.checkin_writer(sock)
        else
          @connection.checkin_reader(sock)
        end
        @checkin_connection = false
      end
    end

    def construct_query_message
      message = BSON::ByteBuffer.new
      message.put_int(@options)
      BSON::BSON_RUBY.serialize_cstr(message, "#{@db.name}.#{@collection.name}")
      message.put_int(@skip)
      message.put_int(@limit)
      spec = query_contains_special_fields? ? construct_query_spec : @selector
      message.put_binary(BSON::BSON_CODER.serialize(spec, false).to_s)
      message.put_binary(BSON::BSON_CODER.serialize(@fields, false).to_s) if @fields
      message
    end

    def instrument_payload
      log = { :database => @db.name, :collection => @collection.name, :selector => selector }
      log[:fields] = @fields if @fields
      log[:skip]   = @skip   if @skip && (@skip != 0)
      log[:limit]  = @limit  if @limit && (@limit != 0)
      log[:order]  = @order  if @order
      log
    end

    def construct_query_spec
      return @selector if @selector.has_key?('$query')
      spec = BSON::OrderedHash.new
      spec['$query']    = @selector
      spec['$orderby']  = Mongo::Support.format_order_clause(@order) if @order
      spec['$hint']     = @hint if @hint && @hint.length > 0
      spec['$explain']  = true if @explain
      spec['$snapshot'] = true if @snapshot
      spec['$maxScan']  = @max_scan if @max_scan
      spec['$returnKey']   = true if @return_key
      spec['$showDiskLoc'] = true if @show_disk_loc
      spec
    end

    # Returns true if the query contains order, explain, hint, or snapshot.
    def query_contains_special_fields?
      @order || @explain || @hint || @snapshot || @show_disk_loc ||
        @max_scan || @return_key
    end

    def close_cursor_if_query_complete
      if @limit > 0 && @returned >= @limit
        close
      end
    end

    def check_modifiable
      if @query_run || @closed
        raise InvalidOperation, "Cannot modify the query once it has been run or closed."
      end
    end
  end
end

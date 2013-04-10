module Mongo

  # A cursor over query results. Returned objects are hashes.
  class Cursor
    include Enumerable
    include Mongo::Constants
    include Mongo::Conversions
    include Mongo::Logging
    include Mongo::ReadPreference

    attr_reader :collection, :selector, :fields,
      :order, :hint, :snapshot, :timeout,
      :full_collection_name, :transformer,
      :options, :cursor_id, :show_disk_loc,
      :comment, :read, :tag_sets, :acceptable_latency

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
      @comment    = opts[:comment]

      # Wire-protocol settings
      @fields     = convert_fields_for_query(opts[:fields])
      @skip       = opts[:skip]     || 0
      @limit      = opts[:limit]    || 0
      @tailable   = opts[:tailable] || false
      @timeout    = opts.fetch(:timeout, true)
      @options    = 0

      # Use this socket for the query
      @socket = opts[:socket]
      @pool   = nil

      @closed       = false
      @query_run    = false

      @transformer = opts[:transformer]
      @read =  opts[:read] || @collection.read
      Mongo::ReadPreference::validate(@read)
      @tag_sets = opts[:tag_sets] || @collection.tag_sets
      @acceptable_latency = opts[:acceptable_latency] || @collection.acceptable_latency

      batch_size(opts[:batch_size] || 0)

      @full_collection_name = "#{@collection.db.name}.#{@collection.name}"
      @cache        = []
      @returned     = 0

      if(!@timeout)
        add_option(OP_QUERY_NO_CURSOR_TIMEOUT)
      end
      if(@read != :primary)
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

      response = @db.command(command, :read => @read, :comment => @comment)
      return response['n'].to_i if Mongo::Support.ok?(response)
      return 0 if response['errmsg'] == "ns missing"
      raise OperationFailure.new("Count failed: #{response['errmsg']}", response['code'], response)
    end

    # Sort this cursor's results.
    #
    # This method overrides any sort order specified in the Collection#find
    # method, and only the last sort applied has an effect.
    #
    # @param [Symbol, Array, Hash, OrderedHash] order either 1) a key to sort by 2)
    #   an array of [key, direction] pairs to sort by or 3) a hash of
    #   field => direction pairs to sort by. Direction should be specified as
    #   Mongo::ASCENDING (or :ascending / :asc) or Mongo::DESCENDING
    #   (or :descending / :desc)
    #
    # @raise [InvalidOperation] if this cursor has already been used.
    #
    # @raise [InvalidSortValueError] if the specified order is invalid.
    def sort(order, direction=nil)
      check_modifiable
      order = [[order, direction]] unless direction.nil?
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
    # This can not override MongoDB's limit on the amount of data it will
    # return to the client. Depending on server version this can be 4-16mb.
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
    # block, if provided. An Enumerator is returned if no block is given.
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
      if block_given? || !defined?(Enumerator)
        while doc = self.next
          yield doc
        end
      else
        Enumerator.new do |yielder|
          while doc = self.next
            yielder.yield doc
          end
        end
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
        @connection.send_message(
          Mongo::Constants::OP_KILL_CURSORS,
          message,
          :pool => @pool
        )
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
      BSON::OrderedHash[
        :selector => @selector,
        :fields   => @fields,
        :skip     => @skip,
        :limit    => @limit,
        :order    => @order,
        :hint     => @hint,
        :snapshot => @snapshot,
        :timeout  => @timeout,
        :max_scan => @max_scan,
        :return_key => @return_key,
        :show_disk_loc => @show_disk_loc,
        :comment  => @comment ]
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

    # Sends initial query -- which is always a read unless it is a command
    #
    # Upon ConnectionFailure, tries query 3 times if socket was not provided
    # and the query is either not a command or is a secondary_ok command.
    #
    # Pins pools upon successful read and unpins pool upon ConnectionFailure
    #
    def send_initial_query
      tries = 0
      instrument(:find, instrument_payload) do
        begin
          message = construct_query_message
          socket = @socket || checkout_socket_from_connection
          results, @n_received, @cursor_id = @connection.receive_message(
            Mongo::Constants::OP_QUERY, message, nil, socket, @command,
            nil, @options & OP_QUERY_EXHAUST != 0)
        rescue ConnectionFailure => ex
          socket.close if socket
          @pool = nil
          @connection.unpin_pool
          @connection.refresh
          if tries < 3 && !@socket && (!@command || Mongo::Support::secondary_ok?(@selector))
            tries += 1
            retry
          else
            raise ex
          end
        rescue OperationFailure, OperationTimeout => ex
          raise ex
        ensure
          socket.checkin unless @socket || socket.nil?
        end
        if !@socket && !@command
          @connection.pin_pool(socket.pool, read_preference)
        end
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

      socket = @pool.checkout

      begin
        results, @n_received, @cursor_id = @connection.receive_message(
          Mongo::Constants::OP_GET_MORE, message, nil, socket, @command, nil)
      ensure
        socket.checkin
      end

      @returned += @n_received
      @cache += results
      close_cursor_if_query_complete
    end

    def checkout_socket_from_connection
      begin
        if @pool
          socket = @pool.checkout
        elsif @command && !Mongo::Support::secondary_ok?(@selector)
          socket = @connection.checkout_reader({:mode => :primary})
        else
          socket = @connection.checkout_reader(read_preference)
        end
      rescue SystemStackError, NoMemoryError, SystemCallError => ex
        @connection.close
        raise ex
      end
      @pool = socket.pool
      socket
    end

    def checkin_socket(sock)
      @connection.checkin(sock)
    end

    def construct_query_message
      message = BSON::ByteBuffer.new("", @connection.max_message_size)
      message.put_int(@options)
      BSON::BSON_RUBY.serialize_cstr(message, "#{@db.name}.#{@collection.name}")
      message.put_int(@skip)
      @batch_size > 1 ? message.put_int(@batch_size) : message.put_int(@limit)
      spec = query_contains_special_fields? ? construct_query_spec : @selector
      message.put_binary(BSON::BSON_CODER.serialize(spec, false, false, @connection.max_bson_size).to_s)
      message.put_binary(BSON::BSON_CODER.serialize(@fields, false, false, @connection.max_bson_size).to_s) if @fields
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
      spec['$comment']  = @comment if @comment
      if needs_read_pref?
        read_pref = Mongo::ReadPreference::mongos(@read, @tag_sets)
        spec['$readPreference'] = read_pref if read_pref
      end
      spec
    end

    def needs_read_pref?
      @connection.mongos? && @read != :primary
    end

    def query_contains_special_fields?
      @order || @explain || @hint || @snapshot || @show_disk_loc ||
        @max_scan || @return_key || @comment || needs_read_pref?
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

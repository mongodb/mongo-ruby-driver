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

require 'mongo/util/byte_buffer'
require 'mongo/util/bson'

module Mongo

  # A cursor over query results. Returned objects are hashes.
  class Cursor
    include Mongo::Conversions

    include Enumerable

    RESPONSE_HEADER_SIZE = 20

    attr_reader :collection, :selector, :admin, :fields, 
      :order, :hint, :snapshot, :timeout,
      :full_collection_name

    # Create a new cursor.
    #
    # Should not be called directly by application developers.
    def initialize(collection, options={})
      @db         = collection.db
      @collection = collection

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

      @full_collection_name   = "#{@collection.db.name}.#{@collection.name}"
      @cache = []
      @closed = false
      @query_run = false
    end

    # Return the next object or nil if there are no more. Raises an error
    # if necessary.
    def next_object
      refill_via_get_more if num_remaining == 0
      o = @cache.shift

      if o && o['$err']
        err = o['$err']

        # If the server has stopped being the master (e.g., it's one of a
        # pair but it has died or something like that) then we close that
        # connection. If the db has auto connect option and a pair of
        # servers, next request will re-open on master server.
        @db.close if err == "not master"

        raise err
      end

      o
    end

    # Get the size of the results set for this query.
    #
    # Returns the number of objects in the results set for this query. Does
    # not take limit and skip into account. Raises OperationFailure on a
    # database error.
    def count
      command = OrderedHash["count",  @collection.name,
                            "query",  @selector,
                            "fields", @fields]
      response = @db.db_command(command)
      return response['n'].to_i if response['ok'] == 1
      return 0 if response['errmsg'] == "ns missing"
      raise OperationFailure, "Count failed: #{response['errmsg']}"
    end

    # Sort this cursor's result
    #
    # Takes either a single key and a direction, or an array of [key,
    # direction] pairs. Directions should be specified as Mongo::ASCENDING
    # or Mongo::DESCENDING (or :ascending or :descending) (or :asc or :desc).
    #
    # Raises InvalidOperation if this cursor has already been used. Raises
    # InvalidSortValueError if specified order is invalid.
    #
    # This method overrides any sort order specified in the Collection#find
    # method, and only the last sort applied has an effect
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

    # Limits the number of results to be returned by this cursor.
    # Returns the current number_to_return if no parameter is given.
    #
    # Raises InvalidOperation if this cursor has already been used.
    #
    # This method overrides any limit specified in the Collection#find method,
    # and only the last limit applied has an effect.
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
    # Raises InvalidOperation if this cursor has already been used.
    #
    # This method overrides any skip specified in the Collection#find method,
    # and only the last skip applied has an effect.
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
    def each
      num_returned = 0
      while more? && (@limit <= 0 || num_returned < @limit)
        yield next_object()
        num_returned += 1
      end
    end

    # Return all of the documents in this cursor as an array of hashes.
    #
    # Raises InvalidOperation if this cursor has already been used (including
    # any previous calls to this method).
    #
    # Use of this method is discouraged - iterating over a cursor is much
    # more efficient in most cases.
    def to_a
      raise InvalidOperation, "can't call Cursor#to_a on a used cursor" if @query_run
      rows = []
      num_returned = 0
      while more? && (@limit <= 0 || num_returned < @limit)
        rows << next_object()
        num_returned += 1
      end
      rows
    end

    # Returns an explain plan record for this cursor.
    def explain
      c = Cursor.new(@collection, query_options_hash.merge(:limit => -@limit.abs, :explain => true))
      explanation = c.next_object
      c.close

      explanation
    end

    # Close the cursor.
    #
    # Note: if a cursor is read until exhausted (read until Mongo::Constants::OP_QUERY or
    # Mongo::Constants::OP_GETMORE returns zero for the cursor id), there is no need to
    # close it by calling this method.
    #
    # Collection#find takes an optional block argument which can be used to
    # ensure that your cursors get closed. See the documentation for
    # Collection#find for details.
    def close
      if @cursor_id
        message = ByteBuffer.new
        message.put_int(0)
        message.put_int(1)
        message.put_long(@cursor_id)
        @db.send_message_with_operation(Mongo::Constants::OP_KILL_CURSORS, message)
      end
      @cursor_id = 0
      @closed    = true
    end

    # Returns true if this cursor is closed, false otherwise.
    def closed?; @closed; end

    # Returns an integer indicating which query options have been selected.
    # See http://www.mongodb.org/display/DOCS/Mongo+Wire+Protocol#MongoWireProtocol-Mongo::Constants::OPQUERY
    def query_opts
      timeout  = @timeout ? 0 : Mongo::Constants::OP_QUERY_NO_CURSOR_TIMEOUT
      slave_ok = @db.slave_ok? ? Mongo::Constants::OP_QUERY_SLAVE_OK : 0 
      slave_ok + timeout
    end

    # Returns the query options set on this Cursor.
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

    # Converts the +:fields+ parameter from a single field name or an array
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

    # Set query selector hash. If the selector is a Code or String object, 
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

    def read_all
      read_message_header
      read_response_header
      read_objects_off_wire
    end

    def read_objects_off_wire
      while doc = next_object_on_wire
        @cache << doc
      end
    end

    def read_message_header
      message = ByteBuffer.new
      message.put_array(@db.receive_full(16).unpack("C*"))
      unless message.size == 16 #HEADER_SIZE
        raise "Short read for DB response header: expected #{16} bytes, saw #{message.size}" 
      end
      message.rewind
      size = message.get_int
      request_id = message.get_int
      response_to = message.get_int
      op = message.get_int
    end

    def read_response_header
      header_buf = ByteBuffer.new
      header_buf.put_array(@db.receive_full(RESPONSE_HEADER_SIZE).unpack("C*"))
      raise "Short read for DB response header; expected #{RESPONSE_HEADER_SIZE} bytes, saw #{header_buf.length}" unless header_buf.length == RESPONSE_HEADER_SIZE
      header_buf.rewind
      @result_flags = header_buf.get_int
      @cursor_id = header_buf.get_long
      @starting_from = header_buf.get_int
      @n_remaining = header_buf.get_int
      if @n_received
        @n_received += @n_remaining
      else
        @n_received = @n_remaining
      end
    end

    def num_remaining
      refill_via_get_more if @cache.length == 0
      @cache.length
    end

    # Internal method, not for general use. Return +true+ if there are
    # more records to retrieve. This methods does not check @limit;
    # #each is responsible for doing that.
    def more?
      num_remaining > 0
    end

    def next_object_on_wire
      # if @n_remaining is 0 but we have a non-zero cursor, there are more
      # to fetch, so do a GetMore operation, but don't do it here - do it
      # when someone pulls an object out of the cache and it's empty
      return nil if @n_remaining == 0
      object_from_stream
    end

    def refill_via_get_more
      return if send_query_if_needed || @cursor_id.zero?
      @db._synchronize {
        message = ByteBuffer.new
        # Reserved.
        message.put_int(0)

        # DB name.
        db_name = @admin ? 'admin' : @db.name
        BSON.serialize_cstr(message, "#{db_name}.#{@collection.name}")

        # Number of results to return; db decides for now.
        message.put_int(0)
        
        # Cursor id.
        message.put_long(@cursor_id)
        @db.send_message_with_operation_without_synchronize(Mongo::Constants::OP_GET_MORE, message)
        read_all
      }
      close_cursor_if_query_complete
    end

    def object_from_stream
      buf = ByteBuffer.new
      buf.put_array(@db.receive_full(4).unpack("C*"))
      buf.rewind
      size = buf.get_int
      buf.put_array(@db.receive_full(size - 4).unpack("C*"), 4)
      @n_remaining -= 1
      buf.rewind
      BSON.new.deserialize(buf)
    end

    def send_query_if_needed
      # Run query first time we request an object from the wire
      if @query_run
        false
      else
        message = construct_query_message(@query)
        @db._synchronize {
          @db.send_message_with_operation_without_synchronize(Mongo::Constants::OP_QUERY, message)
          @query_run = true
          read_all
        }
        close_cursor_if_query_complete
        true
      end
    end

    def construct_query_message(query)
      message = ByteBuffer.new
      message.put_int(query_opts)
      db_name = @admin ? 'admin' : @db.name
      BSON.serialize_cstr(message, "#{db_name}.#{@collection.name}")
      message.put_int(@skip)
      message.put_int(@limit)
      selector = @selector
      if query_contains_special_fields?
        selector = selector_with_special_query_fields
      end
      message.put_array(BSON.new.serialize(selector).to_a)
      message.put_array(BSON.new.serialize(@fields).to_a) if @fields
      message
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
        when String then string_as_sort_parameters(@order)
        when Symbol then symbol_as_sort_parameters(@order)
        when Array  then array_as_sort_parameters(@order)
        when Hash # Should be an ordered hash, but this message doesn't care
          warn_if_deprecated(@order)
          @order 
        else
          raise InvalidSortValueError, "Illegal order_by, '#{@order.class.name}'; must be String, Array, Hash, or OrderedHash"
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

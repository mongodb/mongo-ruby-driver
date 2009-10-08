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

require 'mongo/message'
require 'mongo/util/byte_buffer'
require 'mongo/util/bson'

module Mongo

  # A cursor over query results. Returned objects are hashes.
  class Cursor

    include Enumerable

    RESPONSE_HEADER_SIZE = 20

    attr_reader :db, :collection, :query

    # Create a new cursor.
    #
    # Should not be called directly by application developers.
    def initialize(db, collection, query, admin=false)
      @db, @collection, @query, @admin = db, collection, query, admin
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
      command = OrderedHash["count", @collection.name,
                            "query", @query.selector,
                            "fields", @query.fields()]
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

      @query.order_by = order
      self
    end

    # Limits the number of results to be returned by this cursor.
    #
    # Raises InvalidOperation if this cursor has already been used.
    #
    # This method overrides any limit specified in the Collection#find method,
    # and only the last limit applied has an effect.
    def limit(number_to_return)
      check_modifiable
      raise ArgumentError, "limit requires an integer" unless number_to_return.is_a? Integer

      @query.number_to_return = number_to_return
      self
    end

    # Skips the first +number_to_skip+ results of this cursor.
    #
    # Raises InvalidOperation if this cursor has already been used.
    #
    # This method overrides any skip specified in the Collection#find method,
    # and only the last skip applied has an effect.
    def skip(number_to_skip)
      check_modifiable
      raise ArgumentError, "skip requires an integer" unless number_to_skip.is_a? Integer

      @query.number_to_skip = number_to_skip
      self
    end

    # Iterate over each document in this cursor, yielding it to the given
    # block.
    #
    # Iterating over an entire cursor will close it.
    def each
      num_returned = 0
      while more? && (@query.number_to_return <= 0 || num_returned < @query.number_to_return)
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
      while more? && (@query.number_to_return <= 0 || num_returned < @query.number_to_return)
        rows << next_object()
        num_returned += 1
      end
      rows
    end

    # Returns an explain plan record for this cursor.
    def explain
      limit = @query.number_to_return
      @query.explain = true
      @query.number_to_return = -limit.abs

      c = Cursor.new(@db, @collection, @query)
      explanation = c.next_object
      c.close

      @query.explain = false
      @query.number_to_return = limit
      explanation
    end

    # Close the cursor.
    #
    # Note: if a cursor is read until exhausted (read until OP_QUERY or
    # OP_GETMORE returns zero for the cursor id), there is no need to
    # close it by calling this method.
    #
    # Collection#find takes an optional block argument which can be used to
    # ensure that your cursors get closed. See the documentation for
    # Collection#find for details.
    def close
      @db.send_to_db(KillCursorsMessage.new(@cursor_id)) if @cursor_id
      @cursor_id = 0
      @closed = true
    end

    # Returns true if this cursor is closed, false otherwise.
    def closed?; @closed; end

    private

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
      MessageHeader.new.read_header(@db)
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
      if @query.number_to_return > 0 and @n_received >= @query.number_to_return
        close()
      end
    end

    def num_remaining
      refill_via_get_more if @cache.length == 0
      @cache.length
    end

    # Internal method, not for general use. Return +true+ if there are
    # more records to retrieve. We do not check @query.number_to_return;
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
      if send_query_if_needed or @cursor_id == 0
        return
      end
      @db._synchronize {
        @db.send_to_db(GetMoreMessage.new(@admin ? 'admin' : @db.name, @collection.name, @cursor_id))
        read_all
      }
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
        @db._synchronize {
          @db.send_query_message(QueryMessage.new(@admin ? 'admin' : @db.name, @collection.name, @query))
          @query_run = true
          read_all
        }
        true
      end
    end

    def to_s
      "DBResponse(flags=#@result_flags, cursor_id=#@cursor_id, start=#@starting_from)"
    end

    def check_modifiable
      if @query_run || @closed
        raise InvalidOperation, "Cannot modify the query once it has been run or closed."
      end
    end
  end
end

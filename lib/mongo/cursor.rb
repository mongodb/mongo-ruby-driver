# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2014-2020 MongoDB Inc.
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
  # +Cursor+ objects are not directly exposed to application code. Rather,
  # +Collection::View+ exposes the +Enumerable+ interface to the applications,
  # and the enumerator is backed by a +Cursor+ instance.
  #
  # @example Get an array of 5 users named Emily.
  #   users.find({:name => 'Emily'}).limit(5).to_a
  #
  # @example Call a block on each user doc.
  #   users.find.each { |doc| puts doc }
  #
  # @api private
  class Cursor
    extend Forwardable
    include Enumerable
    include Retryable

    def_delegators :@view, :collection
    def_delegators :collection, :client, :database
    def_delegators :@server, :cluster

    # @return [ Collection::View ] view The collection view.
    attr_reader :view

    # The resume token tracked by the cursor for change stream resuming
    #
    # @return [ BSON::Document | nil ] The cursor resume token.
    # @api private
    attr_reader :resume_token

    # Creates a +Cursor+ object.
    #
    # @example Instantiate the cursor.
    #   Mongo::Cursor.new(view, response, server)
    #
    # @param [ CollectionView ] view The +CollectionView+ defining the query.
    # @param [ Operation::Result ] result The result of the first execution.
    # @param [ Server ] server The server this cursor is locked to.
    # @param [ Hash ] options The cursor options.
    #
    # @option options [ true, false ] :disable_retry Whether to disable
    #   retrying on error when sending getMore operations (deprecated, getMore
    #   operations are no longer retried)
    # @option options [ true, false ] :retry_reads Retry reads (following
    #   the modern mechanism), default is true
    #
    # @since 2.0.0
    def initialize(view, result, server, options = {})
      unless result.is_a?(Operation::Result)
        raise ArgumentError, "Second argument must be a Mongo::Operation::Result: #{result.inspect}"
      end

      @view = view
      @server = server
      @initial_result = result
      @namespace = result.namespace
      @remaining = limit if limited?
      set_cursor_id(result)
      if @cursor_id.nil?
        raise ArgumentError, 'Cursor id must be present in the result'
      end
      @connection_global_id = result.connection_global_id
      @options = options
      @session = @options[:session]
      @explicitly_closed = false
      @lock = Mutex.new
      unless closed?
        register
        ObjectSpace.define_finalizer(self, self.class.finalize(kill_spec(@connection_global_id),
          cluster))
      end
    end

    # @api private
    attr_reader :server

    # @api private
    attr_reader :initial_result

    # Finalize the cursor for garbage collection. Schedules this cursor to be included
    # in a killCursors operation executed by the Cluster's CursorReaper.
    #
    # @param [ Cursor::KillSpec ] kill_spec The KillCursor operation specification.
    # @param [ Mongo::Cluster ] cluster The cluster associated with this cursor and its server.
    #
    # @return [ Proc ] The Finalizer.
    #
    # @api private
    def self.finalize(kill_spec, cluster)
      unless KillSpec === kill_spec
        raise ArgumentError, "First argument must be a KillSpec: #{kill_spec.inspect}"
      end
      proc do
        cluster.schedule_kill_cursor(kill_spec)
      end
    end

    # Get a human-readable string representation of +Cursor+.
    #
    # @example Inspect the cursor.
    #   cursor.inspect
    #
    # @return [ String ] A string representation of a +Cursor+ instance.
    #
    # @since 2.0.0
    def inspect
      "#<Mongo::Cursor:0x#{object_id} @view=#{@view.inspect}>"
    end

    # Iterate through documents returned from the query.
    #
    # A cursor may be iterated at most once. Incomplete iteration is also
    # allowed. Attempting to iterate the cursor more than once raises
    # InvalidCursorOperation.
    #
    # @example Iterate over the documents in the cursor.
    #   cursor.each do |doc|
    #     ...
    #   end
    #
    # @return [ Enumerator ] The enumerator.
    #
    # @since 2.0.0
    def each

      # If we already iterated past the first batch (i.e., called get_more
      # at least once), the cursor on the server side has advanced past
      # the first batch and restarting iteration from the beginning by
      # returning initial result would miss documents in the second batch
      # and subsequent batches up to wherever the cursor is. Detect this
      # condition and abort the iteration.
      #
      # In a future driver version, each would either continue from the
      # end of previous iteration or would always restart from the
      # beginning.
      if @get_more_called
        raise Error::InvalidCursorOperation, 'Cannot restart iteration of a cursor which issued a getMore'
      end

      # To maintain compatibility with pre-2.10 driver versions, reset
      # the documents array each time a new iteration is started.
      @documents = nil

      if block_given?
        # StopIteration raised by try_next ends this loop.
        loop do
          document = try_next
          if explicitly_closed?
            raise Error::InvalidCursorOperation, 'Cursor was explicitly closed'
          end
          yield document if document
        end
        self
      else
        documents = []
        # StopIteration raised by try_next ends this loop.
        loop do
          document = try_next
          if explicitly_closed?
            raise Error::InvalidCursorOperation, 'Cursor was explicitly closed'
          end
          documents << document if document
        end
        documents
      end
    end

    # Return one document from the query, if one is available.
    #
    # This method will wait up to max_await_time_ms milliseconds
    # for changes from the server, and if no changes are received
    # it will return nil. If there are no more documents to return
    # from the server, or if we have exhausted the cursor, it will
    # raise a StopIteration exception.
    #
    # @note This method is experimental and subject to change.
    #
    # @return [ BSON::Document | nil ] A document.
    #
    # @raise [ StopIteration ] Raised on the calls after the cursor had been
    #   completely iterated.
    #
    # @api private
    def try_next
      if @documents.nil?
        # Since published versions of Mongoid have a copy of old driver cursor
        # code, our dup call in #process isn't invoked when Mongoid query
        # cache is active. Work around that by also calling dup here on
        # the result of #process which might come out of Mongoid's code.
        @documents = process(@initial_result).dup
        # the documents here can be an empty array, hence
        # we may end up issuing a getMore in the first try_next call
      end

      if @documents.empty?
        # On empty batches, we cache the batch resume token
        cache_batch_resume_token

        unless closed?
          if exhausted?
            close
            @fully_iterated = true
            raise StopIteration
          end
          @documents = get_more
        else
          @fully_iterated = true
          raise StopIteration
        end
      else
        # cursor is closed here
        # keep documents as an empty array
      end

      # If there is at least one document, cache its _id
      if @documents[0]
        cache_resume_token(@documents[0])
      end

      # Cache the batch resume token if we are iterating
      # over the last document, or if the batch is empty
      if @documents.size <= 1
        cache_batch_resume_token
        if closed?
          @fully_iterated = true
        end
      end

      return @documents.shift
    end

    # Get the batch size.
    #
    # @example Get the batch size.
    #   cursor.batch_size
    #
    # @return [ Integer ] The batch size.
    #
    # @since 2.2.0
    def batch_size
      value = @view.batch_size && @view.batch_size > 0 ? @view.batch_size : limit
      if value == 0
        nil
      else
        value
      end
    end

    # Is the cursor closed?
    #
    # @example Is the cursor closed?
    #   cursor.closed?
    #
    # @return [ true, false ] If the cursor is closed.
    #
    # @since 2.2.0
    def closed?
      # @cursor_id should in principle never be nil
      @cursor_id.nil? || @cursor_id == 0
    end

    # Closes this cursor, freeing any associated resources on the client and
    # the server.
    #
    # @return [ nil ] Always nil.
    def close
      return if closed?

      unregister
      read_with_one_retry do
        spec = {
          coll_name: collection_name,
          db_name: database.name,
          cursor_ids: [id],
        }
        op = Operation::KillCursors.new(spec)
        execute_operation(op)
      end

      nil
    rescue Error::OperationFailure, Error::SocketError, Error::SocketTimeoutError, Error::ServerNotUsable
      # Errors are swallowed since there is noting can be done by handling them.
    ensure
      end_session
      @cursor_id = 0
      @lock.synchronize do
        @explicitly_closed = true
      end
    end

    # Get the parsed collection name.
    #
    # @example Get the parsed collection name.
    #   cursor.coll_name
    #
    # @return [ String ] The collection name.
    #
    # @since 2.2.0
    def collection_name
      # In most cases, this will be equivalent to the name of the collection
      # object in the driver. However, in some cases (e.g. when connected
      # to an Atlas Data Lake), the namespace returned by the find command
      # may be different, which is why we want to use the collection name based
      # on the namespace in the command result.
      if @namespace
        # Often, the namespace will be in the format "database.collection".
        # However, sometimes the collection name will contain periods, which
        # is why this method joins all the namespace components after the first.
        ns_components = @namespace.split('.')
        ns_components[1...ns_components.length].join('.')
      else
        collection.name
      end
    end

    # Get the cursor id.
    #
    # @example Get the cursor id.
    #   cursor.id
    #
    # @note A cursor id of 0 means the cursor was closed on the server.
    #
    # @return [ Integer ] The cursor id.
    #
    # @since 2.2.0
    def id
      @cursor_id
    end

    # Get the number of documents to return. Used on 3.0 and lower server
    # versions.
    #
    # @example Get the number to return.
    #   cursor.to_return
    #
    # @return [ Integer ] The number of documents to return.
    #
    # @since 2.2.0
    def to_return
      use_limit? ? @remaining : (batch_size || 0)
    end

    # Execute a getMore command and return the batch of documents
    # obtained from the server.
    #
    # @return [ Array<BSON::Document> ] The batch of documents
    #
    # @api private
    def get_more
      @get_more_called = true

      # Modern retryable reads specification prohibits retrying getMores.
      # Legacy retryable read logic used to retry getMores, but since
      # doing so may result in silent data loss, the driver no longer retries
      # getMore operations in any circumstance.
      # https://github.com/mongodb/specifications/blob/master/source/retryable-reads/retryable-reads.rst#qa
      process(execute_operation(get_more_operation))
    end

    # @api private
    def kill_spec(connection_global_id)
      KillSpec.new(
        cursor_id: id,
        coll_name: collection_name,
        db_name: database.name,
        connection_global_id: connection_global_id,
        server_address: server.address,
        session: @session,
      )
    end

    # @api private
    def fully_iterated?
      !!@fully_iterated
    end

    private

    def explicitly_closed?
      @lock.synchronize do
        @explicitly_closed
      end
    end

    def batch_size_for_get_more
      if batch_size && use_limit?
        [batch_size, @remaining].min
      else
        batch_size
      end
    end

    def exhausted?
      limited? ? @remaining <= 0 : false
    end

    def cache_resume_token(doc)
      if doc[:_id] && doc[:_id].is_a?(Hash)
        @resume_token = doc[:_id] && doc[:_id].dup.freeze
      end
    end

    def cache_batch_resume_token
      @resume_token = @post_batch_resume_token if @post_batch_resume_token
    end

    def get_more_operation
      spec = {
        session: @session,
        db_name: database.name,
        coll_name: collection_name,
        cursor_id: id,
        # 3.2+ servers use batch_size, 3.0- servers use to_return.
        # TODO should to_return be calculated in the operation layer?
        batch_size: batch_size_for_get_more,
        to_return: to_return,
        max_time_ms: if view.respond_to?(:max_await_time_ms) &&
          view.max_await_time_ms &&
          view.options[:await_data]
        then
          view.max_await_time_ms
        else
          nil
        end,
      }
      if view.respond_to?(:options) && view.options.is_a?(Hash)
        spec[:comment] = view.options[:comment] unless view.options[:comment].nil?
      end
      Operation::GetMore.new(spec)
    end

    def end_session
      @session.end_session if @session && @session.implicit?
    end

    def limited?
      limit ? limit > 0 : false
    end

    def process(result)
      @remaining -= result.returned_count if limited?
      # #process is called for the first batch of results. In this case
      # the @cursor_id may be zero (all results fit in the first batch).
      # Thus we need to check both @cursor_id and the cursor_id of the result
      # prior to calling unregister here.
      unregister if !closed? && result.cursor_id == 0
      @cursor_id = set_cursor_id(result)

      if result.respond_to?(:post_batch_resume_token)
        @post_batch_resume_token = result.post_batch_resume_token
      end

      end_session if closed?

      # Since our iteration code mutates the documents array by calling #shift
      # on it, duplicate the documents here to permit restarting iteration
      # from the beginning of the cursor as long as get_more was not called
      result.documents.dup
    end

    def use_limit?
      limited? && batch_size >= @remaining
    end

    def limit
      @view.send(:limit)
    end

    def register
      cluster.register_cursor(@cursor_id)
    end

    def unregister
      cluster.unregister_cursor(@cursor_id)
    end

    def execute_operation(op)
      context = Operation::Context.new(
        client: client,
        session: @session,
        connection_global_id: @connection_global_id,
      )
      op.execute(@server, context: context)
    end

    # Sets @cursor_id from the operation result.
    #
    # In the operation result cursor id can be represented either as Integer
    # value or as BSON::Int64. This method ensures that the instance variable
    # is always of type Integer.
    #
    # @param [ Operation::Result ] result The result of the operation.
    #
    # @api private
    def set_cursor_id(result)
      @cursor_id = if result.cursor_id.is_a?(BSON::Int64)
                     result.cursor_id.value
                   else
                     result.cursor_id
                   end
    end

  end
end

require 'mongo/cursor/kill_spec'

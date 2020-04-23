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

require 'mongo/cursor/builder'

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
      @view = view
      @server = server
      @initial_result = result
      @remaining = limit if limited?
      @cursor_id = result.cursor_id
      if @cursor_id.nil?
        raise ArgumentError, 'Cursor id must be present in the result'
      end
      @coll_name = nil
      @options = options
      @session = @options[:session]
      unless closed?
        register
        ObjectSpace.define_finalizer(self, self.class.finalize(@cursor_id,
          cluster,
          kill_cursors_op_spec,
          server,
          @session))
      end
    end

    # @api private
    attr_reader :server

    # Finalize the cursor for garbage collection. Schedules this cursor to be included
    # in a killCursors operation executed by the Cluster's CursorReaper.
    #
    # @example Finalize the cursor.
    #   Cursor.finalize(id, cluster, op, server)
    #
    # @param [ Integer ] cursor_id The cursor's id.
    # @param [ Mongo::Cluster ] cluster The cluster associated with this cursor and its server.
    # @param [ Hash ] op_spec The killCursors operation specification.
    # @param [ Mongo::Server ] server The server to send the killCursors operation to.
    #
    # @return [ Proc ] The Finalizer.
    #
    # @since 2.3.0
    def self.finalize(cursor_id, cluster, op_spec, server, session)
      proc do
        cluster.schedule_kill_cursor(cursor_id, op_spec, server)
        session.end_session if session && session.implicit?
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
          yield document if document
        end
        self
      else
        documents = []
        # StopIteration raised by try_next ends this loop.
        loop do
          document = try_next
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
            raise StopIteration
          end
          @documents = get_more
        else
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
      @view.batch_size && @view.batch_size > 0 ? @view.batch_size : limit
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
    #
    # @raise [ Error::OperationFailure ] If the server cursor close fails.
    def close
      return if closed?

      unregister
      read_with_one_retry do
        kill_cursors_operation.execute(@server, client: client)
      end

      nil
    ensure
      end_session
      @cursor_id = 0
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
      @coll_name || collection.name
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
      process(get_more_operation.execute(@server, client: client))
    end

    private

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
      if @server.with_connection { |connection| connection.features }.find_command_enabled?
        spec = Builder::GetMoreCommand.new(self, @session).specification
      else
        spec = Builder::OpGetMore.new(self).specification
      end
      Operation::GetMore.new(spec)
    end

    def end_session
      @session.end_session if @session && @session.implicit?
    end

    def kill_cursors_operation
      Operation::KillCursors.new(kill_cursors_op_spec)
    end

    def kill_cursors_op_spec
      if @server.with_connection { |connection| connection.features }.find_command_enabled?
        Builder::KillCursorsCommand.new(self).specification
      else
        Builder::OpKillCursors.new(self).specification
      end
    end

    def limited?
      limit ? limit > 0 : false
    end

    def process(result)
      @remaining -= result.returned_count if limited?
      @coll_name ||= result.namespace.sub("#{database.name}.", '') if result.namespace
      # #process is called for the first batch of results. In this case
      # the @cursor_id may be zero (all results fit in the first batch).
      # Thus we need to check both @cursor_id and the cursor_id of the result
      # prior to calling unregister here.
      unregister if !closed? && result.cursor_id == 0
      @cursor_id = result.cursor_id

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
  end
end

# Copyright (C) 2014-2017 MongoDB, Inc.
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
    extend Forwardable
    include Enumerable
    include Retryable

    def_delegators :@view, :collection
    def_delegators :collection, :client, :database
    def_delegators :@server, :cluster

    # @return [ Collection::View ] view The collection view.
    attr_reader :view

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
    #   retrying on error when sending getMores.
    #
    # @since 2.0.0
    def initialize(view, result, server, options = {})
      @view = view
      @server = server
      @initial_result = result
      @remaining = limit if limited?
      @cursor_id = result.cursor_id
      @coll_name = nil
      @options = options
      @session = @options[:session]
      register
      ObjectSpace.define_finalizer(self, self.class.finalize(result.cursor_id,
                                                             cluster,
                                                             kill_cursors_op_spec,
                                                             server,
                                                             @session))
    end


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
    # @example Iterate over the documents in the cursor.
    #   cursor.each do |doc|
    #     ...
    #   end
    #
    # @return [ Enumerator ] The enumerator.
    #
    # @since 2.0.0
    def each
      process(@initial_result).each { |doc| yield doc }
      while more?
        return kill_cursors if exhausted?
        get_more.each { |doc| yield doc }
      end
    end

    def try_next
      if @documents.nil?
        @documents = process(@initial_result)
      elsif @documents.empty?
        if more?
          if exhausted?
            kill_cursors
            return nil
          end

          @documents = get_more
        end
      else
        # cursor is closed here
        # keep documents as an empty array
      end

      if @documents
        return @documents.shift
      end

      nil
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
      !more?
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

    private

    def exhausted?
      limited? ? @remaining <= 0 : false
    end

    def get_more
      if @options[:disable_retry]
        process(get_more_operation.execute(@server))
      else
        read_with_retry(@session) do
          process(get_more_operation.execute(@server))
        end
      end
    end

    def get_more_operation
      if @server.features.find_command_enabled?
        spec = Builder::GetMoreCommand.new(self, @session).specification
      else
        spec = Builder::OpGetMore.new(self).specification
      end
      Operation::GetMore.new(spec)
    end

    def kill_cursors
      unregister
      read_with_one_retry do
        kill_cursors_operation.execute(@server)
      end
    ensure
      end_session
      @cursor_id = 0
    end

    def end_session
      @session.end_session if @session && @session.implicit?
    end

    def kill_cursors_operation
      Operation::KillCursors.new(kill_cursors_op_spec)
    end

    def kill_cursors_op_spec
      if @server.features.find_command_enabled?
        Builder::KillCursorsCommand.new(self).specification
      else
        Builder::OpKillCursors.new(self).specification
      end
    end

    def limited?
      limit ? limit > 0 : false
    end

    def more?
      @cursor_id != 0
    end

    def process(result)
      @remaining -= result.returned_count if limited?
      @coll_name ||= result.namespace.sub("#{database.name}.", '') if result.namespace
      unregister if result.cursor_id == 0
      @cursor_id = result.cursor_id
      end_session if !more?
      result.documents
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

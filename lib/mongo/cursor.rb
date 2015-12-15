# Copyright (C) 2014-2015 MongoDB, Inc.
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

    def_delegators :@view, :collection, :limit
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
    #
    # @since 2.0.0
    def initialize(view, result, server)
      @view = view
      @server = server
      @initial_result = result
      @remaining = limit if limited?
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
      read_with_retry do
        process(get_more_operation.execute(@server.context))
      end
    end

    def get_more_operation
      if @server.features.find_command_enabled?
        Operation::Commands::GetMore.new(Builder::GetMoreCommand.new(self).specification)
      else
        Operation::Read::GetMore.new(Builder::OpGetMore.new(self).specification)
      end
    end

    def kill_cursors
      read_with_retry do
        kill_cursors_operation.execute(@server.context)
      end
    end

    def kill_cursors_operation
      if @server.features.find_command_enabled?
        Operation::Commands::Command.new(Builder::KillCursorsCommand.new(self).specification)
      else
        Operation::KillCursors.new(Builder::OpKillCursors.new(self).specification)
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
      @cursor_id = result.cursor_id
      @coll_name ||= result.namespace.sub("#{database.name}.", '') if result.namespace
      result.documents
    end

    def use_limit?
      limited? && batch_size >= @remaining
    end
  end
end

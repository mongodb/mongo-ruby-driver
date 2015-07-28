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

    def_delegators :@view, :collection, :limit
    def_delegators :collection, :client, :database

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

    private

    def batch_size
      @view.batch_size && @view.batch_size > 0 ? @view.batch_size : limit
    end

    def exhausted?
      limited? ? @remaining <= 0 : false
    end

    def get_more
      process(get_more_operation.execute(@server.context))
    end

    def get_more_operation
      Operation::Read::GetMore.new(get_more_spec)
    end

    def get_more_spec
      {
        :to_return => to_return,
        :cursor_id => @cursor_id,
        :db_name   => database.name,
        :coll_name => @coll_name || collection.name
      }
    end

    def kill_cursors
      kill_cursors_operation.execute(@server.context)
    end

    def kill_cursors_operation
      Operation::KillCursors.new(kill_cursors_spec)
    end

    def kill_cursors_spec
      {
        :coll_name => @coll_name || collection.name,
        :db_name => database.name,
        :cursor_ids => [ @cursor_id ]
      }
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

    def to_return
      use_limit? ? @remaining : (batch_size || 0)
    end

    def use_limit?
      limited? && batch_size >= @remaining
    end
  end
end

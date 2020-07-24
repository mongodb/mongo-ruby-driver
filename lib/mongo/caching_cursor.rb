# Copyright (C) 2020 MongoDB Inc.
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

  # A Cursor that attempts to load documents from memory first before hitting
  # the database if the same query has already been executed.
  #
  # @api semiprivate
  class CachingCursor < Cursor

    # @return [ Array <BSON::Document> ] The cursor's cached documents.
    # @api private
    attr_reader :cached_docs

    # @return [ Integer ] The max number of docs to return from the query.
    attr_accessor :limit_docs

    # We iterate over the cached documents if they exist already in the
    # cursor otherwise proceed as normal.
    #
    # @example Iterate over the documents.
    #   cursor.each do |doc|
    #     # ...
    #   end
    def each
      if @cached_docs
        if limit_docs
          @cached_docs.to_a[0...limit_docs.abs].each do |doc|
            yield doc
          end
        else
          @cached_docs.each do |doc|
            yield doc
          end
        end
      else
        super
      end
    end

    # Get a human-readable string representation of +Cursor+.
    #
    # @example Inspect the cursor.
    #   cursor.inspect
    #
    # @return [ String ] A string representation of a +Cursor+ instance.
    def inspect
      "#<Mongo::CachingCursor:0x#{object_id} @view=#{@view.inspect}>"
    end

    private

    def process(result)
      documents = super
      if @cursor_id.zero? && !@after_first_batch
        @cached_docs ||= []
        @cached_docs.concat(documents)
      end
      @after_first_batch = true
      documents
    end
  end
end

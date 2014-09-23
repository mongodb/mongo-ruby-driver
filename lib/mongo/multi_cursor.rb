# Copyright (C) 2009-2014 MongoDB, Inc.
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

  # A wrapper for iterating over multiple cursors synchronously.
  #
  # @since 2.0.0
  class MultiCursor
    include Enumerable

    # @return [ Array<Cursor> ] cursors The wrapped cursors.
    attr_reader :cursors

    # Iterate over the multi cursor, yielding each document in each wrapped
    # cursor.
    #
    # @example Iterate the multi-cursor.
    #   cursor.each do |doc|
    #     ...
    #   end
    #
    # @return [ Enumerator ] The multi-cursor enum.
    #
    # @since 2.0.0
    #
    # @yieldparam [ BSON::Document ] each document.
    def each(&block)
      if block_given?
        iterate_documents{ |doc| yield(doc) }
      else
        Enumerator.new do |y|
          iterate_documents{ |doc| y.yield(doc) }
        end
      end
    end

    # Create the new multi-cursor.
    #
    # @example Create the new multi-cursor.
    #   MultiCursor.new(cursors)
    #
    # @param [ Array<Cursor> ] cursors The wrapped cursors.
    #
    # @since 2.0.0
    def initialize(cursors)
      @cursors = cursors
    end

    private

    def iterate_documents(&block)
      cursors.each do |cursor|
        cursor.each(&block)
      end
    end
  end
end

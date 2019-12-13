# Copyright (C) 2019 MongoDB, Inc.
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
  # A caching cursor stores the documents that have been retrieved from the
  # database in memory, and allows repeatedly iterating these documents without
  # retrieving them from the database each time.
  #
  # To permit concurrent access from multiple threads, once the caching cursor
  # has collected the documents from the first iteration, the only API
  # supported is the #each method. The #try_next method can be called during
  # initial iteration but not after the initial iteration completes
  # (because the caching cursor does not maintain reading position for each
  # consumer - the reading position is implicitly maintained by the Ruby
  # runtime during #each iteration).
  #
  # @api experimental
  class CachingCursor < Cursor
    def each(&block)
      if @iterated
        @cached_documents.each(&block)
      else
        super(&block)
      end
    end

    def try_next
      if @iterated
        raise Error::InvalidCursorOperation, 'Cannot call try_next on a caching cursor past initial iteration'
      else
        begin
          @cached_documents ||= []
          super.tap do |document|
            @cached_documents << document
          end
        rescue StopIteration
          @iterated = true
          raise
        end
      end
    end
  end
end


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
  module Operation
    module Commands
      class ParallelScan

        # Defines custom behaviour of results in a parallel scan.
        #
        # @since 2.0.0
        class Result < Operation::Result

          # The name of the cursors field in the result.
          #
          # @since 2.0.0
          CURSORS = 'cursors'.freeze

          # Get all the cursor ids from the result.
          #
          # @example Get the cursor ids.
          #   result.cursor_ids
          #
          # @return [ Array<Integer> ] The cursor ids.
          #
          # @since 2.0.0
          def cursor_ids
            documents.map{ |doc| doc[CURSOR][CURSOR_ID] }
          end

          # Get the documents from parallel scan.
          #
          # @example Get the documents.
          #   result.documents
          #
          # @return [ Array<BSON::Document> ] The documents.
          #
          # @since 2.0.0
          def documents
            reply.documents[0][CURSORS]
          end

          private

          def first
            @first ||= reply.documents[0] || {}
          end
        end
      end
    end
  end
end

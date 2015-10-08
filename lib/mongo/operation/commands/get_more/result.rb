# Copyright (C) 2015 MongoDB, Inc.
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
      class GetMore

        # Defines custom behaviour of results for the get more command.
        #
        # @since 2.2.0
        class Result < Operation::Result

          # Get the cursor id.
          #
          # @example Get the cursor id.
          #   result.cursor_id
          #
          # @return [ Integer ] The cursor id.
          #
          # @since 2.2.0
          def cursor_id
            cursor_document ? cursor_document[CURSOR_ID] : super
          end

          # Get the documents in the result.
          #
          # @example Get the documents.
          #   result.documents
          #
          # @return [ Array<BSON::Document> ] The documents.
          #
          # @since 2.2.0
          def documents
            cursor_document[NEXT_BATCH]
          end

          private

          def cursor_document
            @cursor_document ||= reply.documents[0][CURSOR]
          end

          def first_document
            @first_document ||= reply.documents[0]
          end
        end
      end
    end
  end
end

# frozen_string_literal: true

# Copyright (C) 2025 MongoDB Inc.
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
    class CursorCommand
      # Parses the cursor field of a command response so a Cursor can be built
      # from the result. The parsing is identical to a find command result.
      #
      # @api private
      class Result < Operation::Result
        # @return [ true | false ] Whether the command response contained a
        #   cursor field.
        def cursor?
          !cursor_document.nil?
        end

        # @return [ Integer | nil ] The cursor id from the cursor document.
        def cursor_id
          cursor? ? cursor_document[CURSOR_ID] : super
        end

        # @return [ Array<BSON::Document> ] The first batch of documents.
        def documents
          cursor? ? cursor_document[FIRST_BATCH] : []
        end

        # @return [ String | nil ] The cursor namespace, "database.collection".
        def namespace
          cursor? ? cursor_document['ns'] : super
        end

        private

        def cursor_document
          return @cursor_document if defined?(@cursor_document)

          @cursor_document = first_document[CURSOR]
        end

        def first_document
          @first_document ||= reply.documents[0]
        end
      end
    end
  end
end

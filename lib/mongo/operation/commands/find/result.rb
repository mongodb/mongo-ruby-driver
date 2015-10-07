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
    class Find

      # Defines custom behaviour of results in find command.
      #
      # @since 2.2.0
      class Result < Operation::Result

        # The field name for the cursor document in an aggregation.
        #
        # @since 2.0.0
        CURSOR = 'cursor'.freeze

        # The cursor id field in the cursor document.
        #
        # @since 2.0.0
        CURSOR_ID = 'id'.freeze

        # The field name for the first batch of a cursor.
        #
        # @since 2.0.0
        FIRST_BATCH = 'firstBatch'.freeze

        def cursor_id
          cursor_document ? cursor_document[CURSOR_ID] : super
        end

        def documents
          cursor_document[FIRST_BATCH]
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

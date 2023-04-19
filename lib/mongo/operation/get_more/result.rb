# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2015-2020 MongoDB Inc.
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
    class GetMore

      # Defines custom behavior of results for the get more command.
      #
      # @since 2.2.0
      # @api semiprivate
      class Result < Operation::Result

        # Get the cursor id.
        #
        # @example Get the cursor id.
        #   result.cursor_id
        #
        # @return [ Integer ] The cursor id.
        #
        # @since 2.2.0
        # @api private
        def cursor_id
          cursor_document ? cursor_document[CURSOR_ID] : super
        end

        # Get the post batch resume token for the result
        #
        # @return [ BSON::Document | nil ] The post batch resume token
        #
        # @api private
        def post_batch_resume_token
          cursor_document ? cursor_document['postBatchResumeToken'] : nil
        end

        # Get the documents in the result.
        #
        # @example Get the documents.
        #   result.documents
        #
        # @return [ Array<BSON::Document> ] The documents.
        #
        # @since 2.2.0
        # @api public
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

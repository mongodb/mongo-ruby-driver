# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2014-2020 MongoDB Inc.
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
    class Aggregate

      # Defines custom behavior of results in an aggregation context.
      #
      # @since 2.0.0
      # @api semiprivate
      class Result < Operation::Result

        # The field name for the aggregation explain information.
        #
        # @since 2.0.5
        # @api private
        EXPLAIN = 'stages'.freeze

        # The legacy field name for the aggregation explain information.
        #
        # @since 2.0.5
        # @api private
        EXPLAIN_LEGACY = 'serverPipeline'.freeze

        # Get the cursor id for the result.
        #
        # @example Get the cursor id.
        #   result.cursor_id
        #
        # @note Even though the wire protocol has a cursor_id field for all
        #   messages of type reply, it is always zero when using the
        #   aggregation framework and must be retrieved from the cursor
        #   document itself. Wahnsinn!
        #
        # @return [ Integer ] The cursor id.
        #
        # @since 2.0.0
        # @api private
        def cursor_id
          cursor_document ? cursor_document[CURSOR_ID] : 0
        end

        # Get the post batch resume token for the result
        #
        # @return [ BSON::Document | nil ] The post batch resume token
        #
        # @api private
        def post_batch_resume_token
          cursor_document ? cursor_document['postBatchResumeToken'] : nil
        end

        # Get the documents for the aggregation result. This is either the
        # first document's 'result' field, or if a cursor option was selected,
        # it is the 'firstBatch' field in the 'cursor' field of the first
        # document returned. Otherwise, it is an explain document.
        #
        # @example Get the documents.
        #   result.documents
        #
        # @return [ Array<BSON::Document> ] The documents.
        #
        # @since 2.0.0
        # @api public
        def documents
          docs = reply.documents[0][RESULT]
          docs ||= cursor_document[FIRST_BATCH] if cursor_document
          docs ||= explain_document
          docs
        end

        private

        # This should only be called on explain responses; it will never
        # return a nil result and will only be meaningful on explain responses
        def explain_document
          first_document[EXPLAIN] || first_document[EXPLAIN_LEGACY] || [first_document]
        end

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

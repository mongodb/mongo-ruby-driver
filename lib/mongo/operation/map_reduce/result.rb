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
    class MapReduce

      # Defines custom behavior of results for a map reduce operation.
      #
      # @since 2.0.0
      # @api semiprivate
      class Result < Operation::Result

        # The counts field for the map/reduce.
        #
        # @since 2.0.0
        # @api private
        COUNTS = 'counts'.freeze

        # The field name for a result without a cursor.
        #
        # @since 2.0.0
        # @api private
        RESULTS = 'results'.freeze

        # The time the operation took constant.
        #
        # @since 2.0.0
        # @api private
        TIME = 'timeMillis'.freeze

        # Gets the map/reduce counts from the reply.
        #
        # @example Get the counts.
        #   result.counts
        #
        # @return [ Hash ] A hash of the result counts.
        #
        # @since 2.0.0
        # @api public
        def counts
          reply.documents[0][COUNTS]
        end

        # Get the documents from the map/reduce.
        #
        # @example Get the documents.
        #   result.documents
        #
        # @return [ Array<BSON::Document> ] The documents.
        #
        # @since 2.0.0
        # @api public
        def documents
          reply.documents[0][RESULTS] || reply.documents[0][RESULT]
        end

        # If the result was a command then determine if it was considered a
        # success.
        #
        # @note If the write was unacknowledged, then this will always return
        #   true.
        #
        # @example Was the command successful?
        #   result.successful?
        #
        # @return [ true, false ] If the command was successful.
        #
        # @since 2.0.0
        # @api public
        def successful?
          !documents.nil?
        end

        # Get the execution time of the map/reduce.
        #
        # @example Get the execution time.
        #   result.time
        #
        # @return [ Integer ] The executing time in milliseconds.
        #
        # @since 2.0.0
        # @api public
        def time
          reply.documents[0][TIME]
        end

        # Validate the result by checking for any errors.
        #
        # @note This only checks for errors with writes since authentication is
        #   handled at the connection level and any authentication errors would
        #   be raised there, before a Result is ever created.
        #
        # @example Validate the result.
        #   result.validate!
        #
        # @raise [ Error::OperationFailure ] If an error is in the result.
        #
        # @return [ Result ] The result if verification passed.
        #
        # @since 2.0.0
        # @api private
        def validate!
          documents.nil? ? raise_operation_failure : self
        end

        # Get the cursor id.
        #
        # @example Get the cursor id.
        #   result.cursor_id
        #
        # @return [ Integer ] Always 0 because map reduce doesn't return a cursor.
        #
        # @since 2.5.0
        # @api private
        def cursor_id
          0
        end

        # Get the number of documents returned by the server in this batch.
        #
        # Map/Reduce operation returns documents inline without using
        # cursors; as such, the standard Mongo::Reply#returned_count does
        # not work correctly for Map/Reduce.
        #
        # Note that the Map/Reduce operation is limited to max BSON document
        # size (16 MB) in its inline result set.
        #
        # @return [ Integer ] The number of documents returned.
        #
        # @api public
        def returned_count
          reply.documents.length
        end

        private

        def first_document
          @first_document ||= reply.documents[0]
        end
      end
    end
  end
end

# Copyright (C) 2014-2015 MongoDB, Inc.
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
    class ListIndexes

      # Defines custom behaviour of results when using the
      # listIndexes command.
      #
      # @since 2.0.0
      class Result < Operation::Result

        # The field name for the cursor document in a listIndexes result.
        #
        # @since 2.0.0
        CURSOR = 'cursor'.freeze

        # The cursor id field in the cursor document.
        #
        # @since 2.0.0
        CURSOR_ID = 'id'.freeze

        # The namespace field in the cursor document.
        #
        # @since 2.0.0
        NAMESPACE = 'ns'.freeze

        # The field name for the first batch of a cursor.
        #
        # @since 2.0.0
        FIRST_BATCH = 'firstBatch'.freeze

        # Get the cursor id for the result.
        #
        # @example Get the cursor id.
        #   result.cursor_id
        #
        # @note Even though the wire protocol has a cursor_id field for all
        #   messages of type reply, it is always zero when using the
        #   listIndexes command and must be retrieved from the cursor
        #   document itself.
        #
        # @return [ Integer ] The cursor id.
        #
        # @since 2.0.0
        def cursor_id
          cursor_document ? cursor_document[CURSOR_ID] : super
        end

        # Get the namespace for the cursor.
        #
        # @example Get the namespace.
        #   result.namespace
        #
        # @return [ String ] The namespace.
        #
        # @since 2.0.0
        def namespace
          cursor_document ? cursor_document[NAMESPACE] : super
        end

        # Get the documents for the listIndexes result. This is the 'firstBatch'
        # field in the 'cursor' field of the first document returned.
        #
        # @example Get the documents.
        #   result.documents
        #
        # @return [ Array<BSON::Document> ] The documents.
        #
        # @since 2.0.0
        def documents
          cursor_document[FIRST_BATCH]
        end

        # Validate the result. In the case where the database or collection
        # does not exist on the server we will get an error, and it's better
        # to raise a meaningful exception here than the ambiguous one when
        # the error occurs.
        #
        # @example Validate the result.
        #   result.validate!
        #
        # @raise [ NoNamespace ] If the ns doesn't exist.
        #
        # @return [ Result ] Self if successful.
        #
        # @since 2.0.0
        def validate!
          !successful? ? raise(Error::OperationFailure.new(parser.message)) : self
        end

        private

        def cursor_document
          @cursor_document ||= first_document[CURSOR]
        end

        def first_document
          @first_document ||= reply.documents[0]
        end
      end
    end
  end
end

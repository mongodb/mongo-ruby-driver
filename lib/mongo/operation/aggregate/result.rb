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
    class Aggregate

      # Defines custom behaviour of results in an aggregation context.
      #
      # @since 2.0.0
      class Result < Operation::Result

        # The field name for a result without a cursor.
        #
        # @since 2.0.0
        RESULT = 'result'.freeze

        # Get the documents for the aggregation result. This is either the
        # first documents' 'result' field, or if a cursor option was selected
        # it is the 'firstBatch' field in the 'cursor' field of the first
        # document returned.
        #
        # @example Get the documents.
        #   result.documents
        #
        # @return [ Array<BSON::Document> ] The documents.
        #
        # @since 2.0.0
        def documents
          reply.documents[0][RESULT]
        end
      end
    end
  end
end

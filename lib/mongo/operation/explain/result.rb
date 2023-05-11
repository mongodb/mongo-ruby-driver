# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2018-2020 MongoDB Inc.
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
    class Explain

      # Defines custom behavior of results in find command with explain.
      #
      # @since 2.5.0
      # @api semiprivate
      class Result < Operation::Result

        # Get the cursor id.
        #
        # @example Get the cursor id.
        #   result.cursor_id
        #
        # @return [ 0 ] Always 0 because explain doesn't return a cursor.
        #
        # @since 2.5.0
        # @api private
        def cursor_id
          0
        end

        # Get the documents in the result.
        #
        # @example Get the documents.
        #   result.documents
        #
        # @return [ Array<BSON::Document> ] The documents.
        #
        # @since 2.5.0
        # @api public
        def documents
          reply.documents
        end
      end
    end
  end
end

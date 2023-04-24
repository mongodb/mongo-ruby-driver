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
    class Update

      # Defines custom behavior of results for an update.
      #
      # @since 2.0.0
      # @api semiprivate
      class Result < Operation::Result

        # The number of modified docs field in the result.
        #
        # @since 2.0.0
        # @api private
        MODIFIED = 'nModified'.freeze

        # The upserted docs field in the result.
        #
        # @since 2.0.0
        # @api private
        UPSERTED = 'upserted'.freeze

        # Get the number of documents matched.
        #
        # @example Get the matched count.
        #   result.matched_count
        #
        # @return [ Integer ] The matched count.
        #
        # @since 2.0.0
        # @api public
        def matched_count
          return 0 unless acknowledged?
          if upsert?
            0
          else
            n
          end
        end

        # Get the number of documents modified.
        #
        # @example Get the modified count.
        #   result.modified_count
        #
        # @return [ Integer ] The modified count.
        #
        # @since 2.0.0
        # @api public
        def modified_count
          return 0 unless acknowledged?
          first[MODIFIED]
        end

        # The identifier of the inserted document if an upsert
        #   took place.
        #
        # @example Get the upserted document's identifier.
        #   result.upserted_id
        #
        # @return [ Object ] The upserted id.
        #
        # @since 2.0.0
        # @api public
        def upserted_id
          return nil unless upsert?
          upsert?.first['_id']
        end

        # Returns the number of documents upserted.
        #
        # @example Get the number of upserted documents.
        #   result.upserted_count
        #
        # @return [ Integer ] The number upserted.
        #
        # @since 2.4.2
        # @api public
        def upserted_count
          upsert? ? n : 0
        end

        # @api public
        def bulk_result
          BulkResult.new(@replies, connection_description)
        end

        private

        def upsert?
          first[UPSERTED]
        end
      end
    end
  end
end

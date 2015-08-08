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
  class BulkWrite

    # Combines bulk write results together.
    #
    # @api private
    #
    # @since 2.1.0
    class ResultCombiner

      # @return [ Hash ] results The results hash.
      attr_reader :results

      # Create the new result combiner.
      #
      # @api private
      #
      # @example Create the result combiner.
      #   ResultCombiner.new
      #
      # @since 2.1.0
      def initialize
        @results = {}
      end

      # Combines a result into the overall results.
      #
      # @api private
      #
      # @example Combine the result.
      #   combiner.combine!(result)
      #
      # @param [ Operation::Result ] result The result to combine.
      #
      # @since 2.1.0
      def combine!(result)
        combine_counts!(result)
        combine_ids!(result)
        combine_errors!(result)
      end

      # Get the final result.
      #
      # @api private
      #
      # @example Get the final result.
      #   combinator.result
      #
      # @return [ BulkWrite::Result ] The final result.
      #
      # @since 2.1.0
      def result
        BulkWrite::Result.new(results).validate!
      end

      private

      def combine_counts!(result)
        Result::FIELDS.each do |field|
          if result.respond_to?(field)
            results.merge!(field => (results[field] || 0) + result.send(field))
          end
        end
      end

      def combine_ids!(result)
        if result.respond_to?(Result::INSERTED_IDS)
          results.merge!(Result::INSERTED_IDS => result.inserted_ids)
        end
        if result.respond_to?(Result::UPSERTED)
          results.merge!(Result::UPSERTED_IDS => result.upserted.map{ |doc| doc['_id'] })
        end
      end

      def combine_errors!(result, indexes = [])
        write_errors = result.aggregate_write_errors(indexes)
        result.validate! unless write_errors
        # The Bulk API only returns the first write concern error encountered.
        write_concern_errors = result.aggregate_write_concern_errors(indexes)
        if write_errors
          results.merge!(
            Error::WRITE_ERRORS => ((results[Error::WRITE_ERRORS] || []) << write_errors).flatten
          )
        end
        if write_concern_errors
          results.merge!(Error::WRITE_CONCERN_ERRORS => write_concern_errors)
        end
      end
    end
  end
end

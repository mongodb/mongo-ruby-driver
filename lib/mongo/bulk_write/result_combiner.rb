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
  class BulkWrite

    # Combines bulk write results together.
    #
    # @api private
    #
    # @since 2.1.0
    class ResultCombiner

      # @return [ Integer ] count The number of documents in the entire batch.
      attr_reader :count

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
        @count = 0
      end

      # Adds a result to the overall results.
      #
      # @api private
      #
      # @example Add the result.
      #   combiner.combine!(result, count)
      #
      # @param [ Operation::Result ] result The result to combine.
      # @param [ Integer ] count The count of requests in the batch.
      #
      # @since 2.1.0
      def combine!(result, count)
        # Errors can be communicated by the server in a variety of fields:
        # writeError, writeErrors, writeConcernError, writeConcernErrors.
        # Currently only errors given in writeConcernErrors will cause
        # counts not to be added, because this behavior is covered by the
        # retryable writes tests. It is possible that some or all of the
        # other errors should also be excluded when combining counts and
        # ids, and it is also possible that only a subset of these error
        # fields is actually possible in the context of bulk writes.
        unless result.write_concern_error?
          combine_counts!(result)
          combine_ids!(result)
        end
        combine_errors!(result)
        @count += count
        @acknowledged = result.acknowledged?
      end

      # Get the final result.
      #
      # @api private
      #
      # @return [ BulkWrite::Result ] The final result.
      #
      # @since 2.1.0
      def result
        BulkWrite::Result.new(results, @acknowledged).validate!
      end

      private

      def combine_counts!(result)
        Result::FIELDS.each do |field|
          if result.respond_to?(field) && value = result.send(field)
            results.merge!(field => (results[field] || 0) + value)
          end
        end
      end

      def combine_ids!(result)
        if result.respond_to?(Result::INSERTED_IDS)
          results[Result::INSERTED_IDS] = (results[Result::INSERTED_IDS] || []) +
                                            result.inserted_ids
        end
        if result.respond_to?(Result::UPSERTED)
          results[Result::UPSERTED_IDS] = (results[Result::UPSERTED_IDS] || []) +
                                            result.upserted.map{ |doc| doc['_id'] }
        end
      end

      def combine_errors!(result)
        combine_write_errors!(result)
        combine_write_concern_errors!(result)
      end

      def combine_write_errors!(result)
        if write_errors = result.aggregate_write_errors(count)
          results.merge!(
            'writeErrors' => ((results['writeErrors'] || []) << write_errors).flatten
          )
        else
          result.validate!
        end
      end

      def combine_write_concern_errors!(result)
        if write_concern_errors = result.aggregate_write_concern_errors(count)
          results['writeConcernErrors'] = (results['writeConcernErrors'] || []) +
                                                   write_concern_errors
        end
      end
    end
  end
end
